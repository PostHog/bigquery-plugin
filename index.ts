import { createBuffer } from '@posthog/plugin-contrib'
import { Plugin, PluginMeta, PluginEvent, PluginJobs } from '@posthog/plugin-scaffold'
import { BigQuery, Table } from '@google-cloud/bigquery'

type BigQueryMeta = PluginMeta<{
    global: {
        buffer: ReturnType<typeof createBuffer>
        eventsToIgnore: Set<string>
        retryQueue: RetryQueue
        bigQueryClient: BigQuery
        bigQueryTable: Table
    }
    config: {
        datasetId: string
        tableId: string
        uploadMinutes: string
        uploadMegabytes: string
        eventsToIgnore: string
    }
}>
type BigQueryPlugin = Plugin<BigQueryMeta>

interface UploadJobPayload {
    batch: PluginEvent[]
    batchId: number
}

class UploadError extends Error {}

class RetryQueue {
    baseInterval: number
    meta: BigQueryMeta
    requestRetriesMap: Map<number, number>

    constructor(meta: BigQueryMeta) {
        this.baseInterval = 3000 // ms
        this.meta = meta
        this.requestRetriesMap = new Map<number, number>()
    }

    async enqueue(batch: PluginEvent[], id: number) {
        const { jobs } = this.meta
        let retriesPerformedSoFar = 0
        if (!this.requestRetriesMap.has(id)) {
            this.requestRetriesMap.set(id, 0)
        } else {
            retriesPerformedSoFar = this.requestRetriesMap.get(id)!
            if (retriesPerformedSoFar === 15) {
                this.requestRetriesMap.delete(id)
                return
            }
            this.requestRetriesMap.set(id, retriesPerformedSoFar + 1)
        }

        const nextRetryMs = 2 ** retriesPerformedSoFar * this.baseInterval
        console.log(`Enqueued batch ${id} for retry in ${nextRetryMs}ms`)

        await jobs.uploadBatchToBigQuery({ batch, batchId: id }).runIn(nextRetryMs, 'milliseconds')
    }
}

export const jobs: PluginJobs<BigQueryMeta> = {
    uploadBatchToBigQuery: async (payload: UploadJobPayload, meta: BigQueryMeta) => {
        const { global } = meta
        try {
            await sendBatchToBigQuery(payload.batch, meta)
        } catch (err) {
            console.error(err)
            global.retryQueue.enqueue(payload.batch, payload.batchId)
        }
    },
}

export const setupPlugin: BigQueryPlugin['setupPlugin'] = async (meta) => {
    const { global, attachments, config, jobs } = meta
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
    const uploadMegabytes = Math.max(1, Math.min(parseInt(config.uploadMegabytes) || 1, 100))
    const uploadMinutes = Math.max(1, Math.min(parseInt(config.uploadMinutes) || 1, 60))

    global.bigQueryClient = new BigQuery({
        projectId: credentials['project_id'],
        credentials,
    })
    global.bigQueryTable = global.bigQueryClient.dataset(config.datasetId).table(config.tableId)

    global.buffer = createBuffer({
        limit: uploadMegabytes * 1024 * 1024,
        timeoutSeconds: uploadMinutes * 60,
        onFlush: async (batch) => {
            await jobs.uploadBatchToBigQuery({ batch, batchId: Math.floor(Math.random() * 1000000) }).runNow()
        },
    })

    global.eventsToIgnore = new Set(
        config.eventsToIgnore ? config.eventsToIgnore.split(',').map((event) => event.trim()) : null
    )

    global.retryQueue = new RetryQueue(meta)

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

export async function onEvent(event: PluginEvent, { global }: BigQueryMeta) {
    if (!global.bigQueryTable) {
        throw new Error('No BigQuery client initialized!')
    }

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

    const parsedEvent = {
        uuid,
        eventName,
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

    if (!global.eventsToIgnore.has(eventName)) {
        global.buffer.add(parsedEvent)
    }
}

export async function sendBatchToBigQuery(rows: PluginEvent[], { global }: BigQueryMeta) {
    console.log(`Uploading ${rows.length} event ${rows.length > 1 ? 'rows' : 'row'} to BigQuery`)
    try {
        await global.bigQueryTable.insert(rows)
    } catch (error) {
        throw new UploadError(`Error inserting into BigQuery! ${JSON.stringify(error.errors)}`)
    }
}
