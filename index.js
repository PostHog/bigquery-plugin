async function setupPlugin({ global, attachments, config }) {
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
    global.bigQueryClient = new google.cloud.bigquery.BigQuery({
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
            const [table] = await global.bigQueryClient
                .dataset(config.datasetId)
                .createTable(config.tableId, { schema })
        } catch (error) {
            // a different worker already created the table
            if (!error.message.includes('Already Exists')) {
                throw new Error()
            }
        }
    }
}

async function processEventBatch(batch, { config, global }) {
    if (!global.bigQueryTable) {
        throw new Error('No BigQuery client initialized!')
    }

    const rows = batch.map((oneEvent) => {
        const { event, properties, $set, $set_once, distinct_id, team_id, site_url, now, sent_at, uuid, ..._discard } = oneEvent
        const ip = properties?.['$ip'] || oneEvent.ip
        const timestamp = oneEvent.timestamp || oneEvent.data?.timestamp || properties?.timestamp || now || sent_at
        let ingestedProperties = properties
        let elements = []

        // only move prop to elements for the $autocapture action
        if (event === '$autocapture' && properties['$elements']) {
            const { $elements, ...props } = properties
            ingestedProperties = props
            elements = $elements
        }

        return {
            uuid,
            event,
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

    try {
        await global.bigQueryTable.insert(rows)
    } catch (error) {
        throw new Error(`Error inserting into BigQuery! ${JSON.stringify(error.errors)}`)
    }

    return batch
}