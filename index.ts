import { Plugin, RetryError } from '@posthog/plugin-scaffold'
import { BigQuery, Table, TableField, TableMetadata } from '@google-cloud/bigquery'

type BigQueryPlugin = Plugin<{
    global: {
        bigQueryClient: BigQuery
        bigQueryTable: Table
        bigQueryTableFields: TableField[]
        exportEventsToIgnore: Set<string>
    }
    config: {
        datasetId: string
        tableId: string
        exportEventsToIgnore: string
        exportElementsOnAnyEvent: 'Yes' | 'No'
    }
}>


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
                `Inserted ${events.length} ${events.length > 1 ? 'events' : 'event'} to BigQuery. Took ${
                    end / 1000
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