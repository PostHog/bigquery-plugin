import { createBuffer } from '@posthog/plugin-contrib'
import { Plugin, PluginMeta, ProcessedPluginEvent, RetryError } from '@posthog/plugin-scaffold'
import { BigQuery, Table, TableField, TableMetadata } from '@google-cloud/bigquery'

type BigQueryPlugin = Plugin<{
    global: {
        bigQueryClient: BigQuery
        bigQueryTable: Table
        bigQueryTableFields: TableField[]

        exportEventsBuffer: ReturnType<typeof createBuffer>
        exportEventsToIgnore: Set<string>
        exportEventsWithRetry: (payload: UploadJobPayload, meta: PluginMeta<BigQueryPlugin>) => Promise<void>
    }
    config: {
        datasetId: string
        tableId: string

        exportEventsBufferBytes: string
        exportEventsBufferSeconds: string
        exportEventsToIgnore: string
        exportElementsOnAnyEvent: 'Yes' | 'No'
    }
    jobs: {
        exportEventsWithRetry: UploadJobPayload
    }
}>

interface UploadJobPayload {
    batch: ProcessedPluginEvent[]
    batchId: number
    retriesPerformedSoFar: number
}

export const setupPlugin: BigQueryPlugin['setupPlugin'] = async (meta) => {
    const { global, attachments, config } = meta
    if (!attachments.googleCloudKeyJson) {
        throw new Error('JSON config not provided!')
    }
    if (!config.datasetId) {
        throw new Error('Dataset ID not provided!')
    }
    if (!config.tableId) {
        throw new Error('Table ID not provided!')
    }

    const credentials = JSON.parse(attachments.googleCloudKeyJson.contents.toString())
    global.bigQueryClient = new BigQuery({
        projectId: credentials['project_id'],
        credentials,
        autoRetry: false,
    })
    global.bigQueryTable = global.bigQueryClient.dataset(config.datasetId).table(config.tableId)

    global.bigQueryTableFields = [
        { name: 'uuid', type: 'STRING' },
        { name: 'event', type: 'STRING' },
        { name: 'properties', type: 'STRING' },
        { name: 'elements', type: 'STRING' },
        { name: 'set', type: 'STRING' },
        { name: 'set_once', type: 'STRING' },
        { name: 'distinct_id', type: 'STRING' },
        { name: 'team_id', type: 'INT64' },
        { name: 'ip', type: 'STRING' },
        { name: 'site_url', type: 'STRING' },
        { name: 'timestamp', type: 'TIMESTAMP' },
        { name: 'bq_ingested_timestamp', type: 'TIMESTAMP' },
    ]

    try {
        const [metadata]: TableMetadata[] = await global.bigQueryTable.getMetadata()

        if (!metadata.schema || !metadata.schema.fields) {
            throw new Error('Can not get metadata for table. Please check if the table schema is defined.')
        }

        const existingFields = metadata.schema.fields
        const fieldsToAdd = global.bigQueryTableFields.filter(
            ({ name }) => !existingFields.find((f: any) => f.name === name)
        )

        if (fieldsToAdd.length > 0) {
            console.info(
                `Incomplete schema on BigQuery table! Adding the following fields to reach parity: ${JSON.stringify(
                    fieldsToAdd
                )}`
            )

            let result: TableMetadata
            try {
                metadata.schema.fields = metadata.schema.fields.concat(fieldsToAdd)
                ;[result] = await global.bigQueryTable.setMetadata(metadata)
            } catch (error) {
                const fieldsToStillAdd = global.bigQueryTableFields.filter(
                    ({ name }) => !result.schema?.fields?.find((f: any) => f.name === name)
                )

                if (fieldsToStillAdd.length > 0) {
                    throw new Error(
                        `Tried adding fields ${JSON.stringify(fieldsToAdd)}, but ${JSON.stringify(
                            fieldsToStillAdd
                        )} still to add. Can not start plugin.`
                    )
                }
            }
        }
    } catch (error) {
        // some other error? abort!
        if (!(error as Error).message.includes('Not found')) {
            throw error
        }
        console.log(`Creating BigQuery Table - ${config.datasetId}:${config.tableId}`)

        try {
            await global.bigQueryClient
                .dataset(config.datasetId)
                .createTable(config.tableId, { schema: global.bigQueryTableFields })
        } catch (error) {
            // a different worker already created the table
            if (!(error as Error).message.includes('Already Exists')) {
                throw error
            }
        }
    }

    setupBufferExportCode(meta, exportEventsToBigQuery)
}

export async function exportEventsToBigQuery(events: ProcessedPluginEvent[], { global, config }: PluginMeta<BigQueryPlugin>) {
    const insertOptions = {
        createInsertId: false,
        partialRetries: 0,
        raw: true,
    }

    if (!global.bigQueryTable) {
        throw new Error('No BigQuery client initialized!')
    }
    try {
        const rows = events.map((event) => {
            const {
                event: eventName,
                properties,
                $set,
                $set_once,
                distinct_id,
                team_id,
                uuid,
                elements,
                ..._discard
            } = event
            const ip = properties?.['$ip'] || event.ip
            const timestamp = event.timestamp || properties?.timestamp
            let ingestedProperties = properties

            const shouldExportElementsForEvent =
                eventName === '$autocapture' || config.exportElementsOnAnyEvent === 'Yes'
 

            const object: { json: Record<string, any>; insertId?: string } = {
                json: {
                    uuid,
                    event: eventName,
                    properties: JSON.stringify(ingestedProperties || {}),
                    elements: JSON.stringify(shouldExportElementsForEvent && elements ? elements : {}),
                    set: JSON.stringify($set || {}),
                    set_once: JSON.stringify($set_once || {}),
                    distinct_id,
                    team_id,
                    ip,
                    timestamp: timestamp,
                    bq_ingested_timestamp: new Date().toISOString(),
                },
            }
            return object
        })

        const start = Date.now()
        await global.bigQueryTable.insert(rows, insertOptions)
        const end = Date.now() - start

        console.log(
            `Inserted ${events.length} ${events.length > 1 ? 'events' : 'event'} to BigQuery. Took ${
                end / 1000
            } seconds.`
        )
    } catch (error) {
        console.error(
            `Error inserting ${events.length} ${events.length > 1 ? 'events' : 'event'} into BigQuery: `,
            error
        )
        throw new RetryError(`Error inserting into BigQuery! ${(error as Error).message}`)
    }
}

// What follows is code that should be abstracted away into the plugin server itself.

const setupBufferExportCode = (
    meta: PluginMeta<BigQueryPlugin>,
    exportEvents: (events: ProcessedPluginEvent[], meta: PluginMeta<BigQueryPlugin>) => Promise<void>
) => {
    const uploadBytes = Math.max(
        1024 * 1024,
        Math.min(parseInt(meta.config.exportEventsBufferBytes) || 1024 * 1024, 1024 * 1024 * 10)
    )
    const uploadSeconds = Math.max(1, Math.min(parseInt(meta.config.exportEventsBufferSeconds) || 30, 600))

    meta.global.exportEventsToIgnore = new Set(
        meta.config.exportEventsToIgnore
            ? meta.config.exportEventsToIgnore.split(',').map((event) => event.trim())
            : null
    )
    meta.global.exportEventsBuffer = createBuffer({
        limit: uploadBytes,
        timeoutSeconds: uploadSeconds,
        onFlush: async (batch) => {
            const jobPayload = {
                batch,
                batchId: Math.floor(Math.random() * 1000000),
                retriesPerformedSoFar: 0,
            }
            const firstThroughQueue = false // TODO: might make sense sometimes? e.g. when we are processing too many tasks already?
            if (firstThroughQueue) {
                await meta.jobs.exportEventsWithRetry(jobPayload).runNow()
            } else {
                await meta.global.exportEventsWithRetry(jobPayload, meta)
            }
        },
    })
    meta.global.exportEventsWithRetry = async (payload: UploadJobPayload, meta: PluginMeta<BigQueryPlugin>) => {
        const { jobs } = meta
        try {
            await exportEvents(payload.batch, meta)
        } catch (err) {
            if (err instanceof RetryError) {
                if (payload.retriesPerformedSoFar < 15) {
                    const nextRetrySeconds = 2 ** payload.retriesPerformedSoFar * 3
                    console.log(`Enqueued batch ${payload.batchId} for retry in ${Math.round(nextRetrySeconds)}s`)

                    await jobs
                        .exportEventsWithRetry({ ...payload, retriesPerformedSoFar: payload.retriesPerformedSoFar + 1 })
                        .runIn(nextRetrySeconds, 'seconds')
                } else {
                    console.log(
                        `Dropped batch ${payload.batchId} after retrying ${payload.retriesPerformedSoFar} times`
                    )
                }
            } else {
                throw err
            }
        }
    }
}

export const jobs: BigQueryPlugin['jobs'] = {
    exportEventsWithRetry: async (payload, meta) => {
        await meta.global.exportEventsWithRetry(payload, meta)
    },
}

export const onEvent: BigQueryPlugin['onEvent'] = (event, { global }) => {
    if (!global.exportEventsToIgnore.has(event.event)) {
        global.exportEventsBuffer.add(event, JSON.stringify(event).length)
    }
}
