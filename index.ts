import { createBuffer } from '@posthog/plugin-contrib'
import { Plugin, PluginMeta, PluginEvent } from '@posthog/plugin-scaffold'
import { BigQuery, Table } from '@google-cloud/bigquery'

type BigQueryPlugin = Plugin<{
    global: {
        bigQueryClient: BigQuery
        bigQueryTable: Table

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
    }
    jobs: {
        exportEventsWithRetry: UploadJobPayload
    }
}>

interface UploadJobPayload {
    batch: PluginEvent[]
    batchId: number
    retriesPerformedSoFar: number
}

class RetryError extends Error {}

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
    })
    global.bigQueryTable = global.bigQueryClient.dataset(config.datasetId).table(config.tableId)

    try {
        // check if the table exists
        await global.bigQueryTable.get()
    } catch (error) {
        // some other error? abort!
        if (!error.message.includes('Not found')) {
            throw new Error(error)
        }
        console.log(`Creating BigQuery Table - ${config.datasetId}:${config.tableId}`)

        const schema = [
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
        ]

        try {
            await global.bigQueryClient.dataset(config.datasetId).createTable(config.tableId, { schema })
        } catch (error) {
            // a different worker already created the table
            if (!error.message.includes('Already Exists')) {
                throw new Error()
            }
        }
    }

    setupBufferExportCode(meta, exportEventsToBigQuery)
}

export async function exportEventsToBigQuery(events: PluginEvent[], { global }: PluginMeta<BigQueryPlugin>) {
    if (!global.bigQueryTable) {
        throw new Error('No BigQuery client initialized!')
    }
    console.log(`Uploading ${events.length} event ${events.length > 1 ? 'events' : 'row'} to BigQuery`)
    try {
        const rows = events.map((event) => {
            const {
                event: eventName,
                properties,
                $set,
                $set_once,
                distinct_id,
                team_id,
                site_url,
                now,
                sent_at,
                uuid,
                ..._discard
            } = event
            const ip = properties?.['$ip'] || event.ip
            const timestamp = event.timestamp || properties?.timestamp || now || sent_at
            let ingestedProperties = properties
            let elements = []

            // only move prop to elements for the $autocapture action
            if (eventName === '$autocapture' && properties && '$elements' in properties) {
                const { $elements, ...props } = properties
                ingestedProperties = props
                elements = $elements
            }

            return {
                uuid,
                event: eventName,
                properties: JSON.stringify(ingestedProperties || {}),
                elements: JSON.stringify(elements || {}),
                set: JSON.stringify($set || {}),
                set_once: JSON.stringify($set_once || {}),
                distinct_id,
                team_id,
                ip,
                site_url,
                timestamp: timestamp ? global.bigQueryClient.timestamp(timestamp) : null,
            }
        })
        await global.bigQueryTable.insert(rows)
    } catch (error) {
        throw new RetryError(`Error inserting into BigQuery! ${JSON.stringify(error.errors)}`)
    }
}

// What follows is code that should be abstracted away into the plugin server itself.

const setupBufferExportCode = (
    meta: PluginMeta<BigQueryPlugin>,
    exportEvents: (events: PluginEvent[], meta: PluginMeta<BigQueryPlugin>) => Promise<void>
) => {
    const uploadBytes = Math.max(1, Math.min(parseInt(meta.config.exportEventsBufferBytes) || 1024 * 1024, 100))
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
        meta.global.exportEventsWithRetry(payload, meta)
    },
}

export const onEvent: BigQueryPlugin['onEvent'] = (event, { global }) => {
    if (!global.exportEventsToIgnore.has(event.event)) {
        global.exportEventsBuffer.add(event)
    }
}
