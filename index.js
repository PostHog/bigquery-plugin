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
            { name: 'event', type: 'STRING' },
            { name: 'properties', type: 'STRING' },
            { name: 'set', type: 'STRING' },
            { name: 'ip', type: 'STRING' },
            { name: 'site_url', type: 'STRING' },
            { name: 'now', type: 'TIMESTAMP' },
            { name: 'sent_at', type: 'TIMESTAMP' },
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
        const { event, properties, $set, ip, site_url, now, sent_at, ...misc } = oneEvent
        const timestamp = oneEvent.timestamp || oneEvent.data?.timestamp || properties?.timestamp

        return {
            event,
            properties: JSON.stringify(properties || {}),
            set: JSON.stringify($set || {}),
            ip,
            site_url,
            now: now ? global.bigQueryClient.timestamp(now) : null,
            sent_at: sent_at ? global.bigQueryClient.timestamp(sent_at) : null,
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
