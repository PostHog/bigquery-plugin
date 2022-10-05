import { Plugin, RetryError } from '@posthog/plugin-scaffold'
import { BigQuery, Table, TableField, TableMetadata } from '@google-cloud/bigquery'
import { FetchError } from 'node-fetch'

type BigQueryPlugin = Plugin<{
    global: {
        bigQueryClient: BigQuery
        bigQueryTable: Table
        exportEventsToIgnore: Set<string>
    }
    config: {
        datasetId: string
        tableId: string
        exportEventsToIgnore: string
        exportElementsOnAnyEvent: 'Yes' | 'No'
    }
}>

export const BIG_QUERY_TABLE_FIELDS: TableField[] = [
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

    meta.global.exportEventsToIgnore = new Set(
        meta.config.exportEventsToIgnore
            ? meta.config.exportEventsToIgnore.split(',').map((event) => event.trim())
            : null
    )

    const credentials = JSON.parse(attachments.googleCloudKeyJson.contents.toString())

    try {
        global.bigQueryClient = new BigQuery({
            projectId: credentials['project_id'],
            credentials,
            autoRetry: false,
        })

        global.bigQueryTable = global.bigQueryClient.dataset(config.datasetId).table(config.tableId)

        // Note: When changing table schema in incompatible ways remember to cache-bust this.
        const cachedMetadata = await meta.cache.get('cachedMetadata', null) as any | null

        if (cachedMetadata?.datasetId === config.datasetId && cachedMetadata?.tableId === config.tableId && cachedMetadata?.existingFields === BIG_QUERY_TABLE_FIELDS.length) {
            return
        }

        let metadata: TableMetadata
        try {
            [metadata] = await global.bigQueryTable.getMetadata()
        } catch (error) {
            console.error("Failed to get metadata:", error)
            // some other error? abort!
            if (!(error as Error).message.includes('Not found')) {
                throw error
            }
            createBigQueryTable(meta)
            return // if we just created the table, we don't need to verify nor update the schema
        }

        updateBigQueryTableSchema(meta, metadata)

    } catch (error) {
        if(error instanceof Error) {
            console.error('Error encountered in setupPlugin:', error.stack)
        } else {
            console.error('Error encountered in setupPlugin:', error)
        }

        if (error instanceof FetchError) {
            // If we get an operational fetch error then indicate that
            // setupPlugin should be retried by raising a retryError. node-fetch
            // (which the Google auth lib uses) raises a FetchError for
            // "operational" errors e.g. failed sockets.
            //
            // See
            // https://github.com/node-fetch/node-fetch/blob/main/docs/ERROR-HANDLING.md
            // for details.
            throw new RetryError(`Operational fetch error encountered: ${(error as Error).message}`)
        } else {
            // Otherwise we just throw and let the caller decide what to do.
            throw error
        }
    }
}

async function updateBigQueryTableSchema(meta: any, metadata: TableMetadata) {
        const { global, config } = meta

        if (!metadata.schema || !metadata.schema.fields) {
            throw new Error('Can not get metadata for table. Please check if the table schema is defined.')
        }
        try {
            const existingFields = metadata.schema.fields
            const fieldsToAdd = BIG_QUERY_TABLE_FIELDS.filter(
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
                    console.error("Failed to set Metadata", error)

                    // a different worker already updated the table
                    const fieldsToStillAdd = BIG_QUERY_TABLE_FIELDS.filter(
                        ({ name }) => !result.schema?.fields?.find((f: any) => f.name === name)
                    )

                    if (fieldsToStillAdd.length > 0) {
                        console.error(`Tried adding fields ${JSON.stringify(fieldsToAdd)}, but ${JSON.stringify(
                            fieldsToStillAdd
                        )} still to add. Can not start plugin.`)
                        throw error
                    }
                }
            }
        } catch (error) {
            console.error("Updating table schema failed:", error)
            throw error
        }
        // Always refresh the cache
        await meta.cache.set('cachedMetadata', {
            tableId: config.tableId,
            datasetId: config.datasetId,
            existingFields: BIG_QUERY_TABLE_FIELDS.length
        })
}

async function createBigQueryTable(meta: any) {
    const { global, config } = meta

    console.log(`Creating BigQuery Table - ${config.datasetId}:${config.tableId}`)

    try {
        await global.bigQueryClient
            .dataset(config.datasetId)
            .createTable(config.tableId, { schema: BIG_QUERY_TABLE_FIELDS })

        await meta.cache.set('cachedMetadata', {
            tableId: config.tableId,
            datasetId: config.datasetId,
            existingFields: BIG_QUERY_TABLE_FIELDS.length
        })
    } catch (error) {
        // a different worker already created the table
        if (!(error as Error).message.includes('Already Exists')) {
            throw new RetryError(`Another thread aleady created the table, retrying setup (${(error as Error).message})`)
        }
        console.error('Creating BigQuery Table failed:', error)
        throw error
    }
}


export const exportEvents: BigQueryPlugin['exportEvents'] = async (events, { global, config }) => {
    const insertOptions = {
        createInsertId: false,
        partialRetries: 0,
        raw: true,
    }

    if (!global.bigQueryTable) {
        throw new Error('No BigQuery client initialized!')
    }
    try {

        const rows = []

        for (const event of events) {
            if (global.exportEventsToIgnore.has(event.event)) {
                continue
            }

            const {
                event: eventName,
                properties,
                $set,
                $set_once,
                distinct_id,
                team_id,
                uuid,
                timestamp,
                elements,
                ..._discard
            } = event
            const ip = properties?.['$ip'] || event.ip
            let ingestedProperties = properties

            const shouldExportElementsForEvent =
                eventName === '$autocapture' || config.exportElementsOnAnyEvent === 'Yes'


            const elementsToExport = shouldExportElementsForEvent ? elements : []
            const object: { json: Record<string, any>; insertId?: string } = {
                json: {
                    uuid,
                    event: eventName,
                    properties: JSON.stringify(ingestedProperties || {}),
                    elements: JSON.stringify(elementsToExport),
                    set: JSON.stringify($set || {}),
                    set_once: JSON.stringify($set_once || {}),
                    distinct_id,
                    team_id,
                    ip,
                    site_url: '',
                    timestamp: timestamp,
                    bq_ingested_timestamp: new Date().toISOString(),
                },
            }

            rows.push(object)
        }



        if (rows.length > 0) {
            const start = Date.now()
            await global.bigQueryTable.insert(rows, insertOptions)
            const end = Date.now() - start

            console.log(
                `Inserted ${events.length} ${events.length > 1 ? 'events' : 'event'} to BigQuery. Took ${end / 1000
                } seconds.`
            )
        }

    } catch (error) {
        console.error(
            `Error inserting ${events.length} ${events.length > 1 ? 'events' : 'event'} into BigQuery: `,
            error
        )
        throw new RetryError(`Error inserting into BigQuery! ${(error as Error).message}`)
    }
}