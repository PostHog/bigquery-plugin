import { Plugin, PluginMeta, PluginEvent, RetryError } from '@posthog/plugin-scaffold'
import { BigQuery, Table } from '@google-cloud/bigquery'

type BigQueryPlugin = Plugin<{
    global: {
        bigQueryClient: BigQuery
        bigQueryTable: Table
    }
    config: {
        datasetId: string
        tableId: string
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
}

export async function exportEvents(events: PluginEvent[], { global }: PluginMeta<BigQueryPlugin>) {
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
        console.log(`Inserted ${events.length} ${events.length > 1 ? 'events' : 'event'} to BigQuery`)
    } catch (error) {
        console.error(
            `Error inserting ${events.length} ${events.length > 1 ? 'events' : 'event'} into BigQuery: `,
            error
        )
        throw new RetryError(`Error inserting into BigQuery! ${JSON.stringify(error.errors)}`)
    }
}
