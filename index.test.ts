import { BIG_QUERY_TABLE_FIELDS, exportEvents, setupPlugin } from './index'

const mockedBigQueryTable = {
    insert: jest.fn(),
    getMetadata: jest.fn(() => [{ schema: { fields: []} }]),
    setMetadata: jest.fn(() => [])
}

const mockedDataset = {
    table: () => mockedBigQueryTable,
    createTable: jest.fn()
}

jest.mock('@google-cloud/bigquery', () => ({
    BigQuery: jest.fn(() => ({
        dataset: () => mockedDataset
    }))
}))

describe('BigQuery Export Plugin', () => {
    let meta: Record<string, any>

    beforeEach(() => {
        meta = {
            config: {
                exportElementsOnAnyEvent: 'No',
                datasetId: '1234',
                tableId: '1234',
            },
            attachments: {
                googleCloudKeyJson: {
                    contents: `{ "foo": "some secret stuff" }`,
                },
            },
            global: {
                bigQueryTable: mockedBigQueryTable,
                exportEventsToIgnore: new Set(['ignore me']),
            },
        }
        jest.clearAllMocks()
    })

    describe('setupPlugin()', () => {
        test('creates table if error thrown when getting metadata on a non-existent table', () => {
            setupPlugin?.(meta as any)
            expect(mockedDataset.createTable).not.toHaveBeenCalled()

            mockedBigQueryTable.getMetadata.mockImplementationOnce(() => {throw new Error('Not found')})
            setupPlugin?.(meta as any)
            expect(mockedDataset.createTable).toHaveBeenCalled()
        })

        test('does no table updates if all fields already exist', () => {
            mockedBigQueryTable.getMetadata.mockImplementationOnce(() => [{ schema: { fields: BIG_QUERY_TABLE_FIELDS as any } }])

            setupPlugin?.(meta as any)
            expect(mockedBigQueryTable.setMetadata).not.toHaveBeenCalled()
            expect(mockedDataset.createTable).not.toHaveBeenCalled()
        })

    })

    describe('exportEvents()', () => {
        test('makes the right call to bigQueryTable.insert', async () => {
            await exportEvents?.(
                [
                    {
                        event: 'test',
                        properties: {},
                        distinct_id: 'did1',
                        team_id: 1,
                        uuid: '37114ebb-7b13-4301-b849-0d0bd4d5c7e5',
                        ip: '127.0.0.1',
                        timestamp: '2022-08-18T15:42:32.597Z',
                    },
                    {
                        event: 'test2',
                        properties: {},
                        distinct_id: 'did1',
                        team_id: 1,
                        uuid: '37114ebb-7b13-4301-b859-0d0bd4d5c7e5',
                        ip: '127.0.0.1',
                        timestamp: '2022-08-18T15:42:32.597Z',
                        elements: [{ attr_id: 'haha' }],
                    },
                ],
                meta as any
            )

            expect(meta.global.bigQueryTable.insert).toHaveBeenCalledWith(
                [
                    {
                        json: {
                            bq_ingested_timestamp: expect.any(String),
                            distinct_id: 'did1',
                            elements: '[]',
                            event: 'test',
                            ip: '127.0.0.1',
                            properties: '{}',
                            set: '{}',
                            set_once: '{}',
                            site_url: '',
                            team_id: 1,
                            timestamp: '2022-08-18T15:42:32.597Z',
                            uuid: '37114ebb-7b13-4301-b849-0d0bd4d5c7e5',
                        },
                    },
                    {
                        json: {
                            bq_ingested_timestamp: expect.any(String),
                            distinct_id: 'did1',
                            elements: '[]',
                            event: 'test2',
                            ip: '127.0.0.1',
                            properties: '{}',
                            set: '{}',
                            set_once: '{}',
                            site_url: '',
                            team_id: 1,
                            timestamp: '2022-08-18T15:42:32.597Z',
                            uuid: '37114ebb-7b13-4301-b859-0d0bd4d5c7e5',
                        },
                    },
                ],
                {
                    createInsertId: false,
                    partialRetries: 0,
                    raw: true,
                }
            )
        }),
            test('ignores events in exportEventsToIgnore', async () => {
                const meta = {
                    config: {
                        exportElementsOnAnyEvent: 'No',
                    },
                    global: {
                        bigQueryTable: {
                            insert: jest.fn(),
                        },
                        exportEventsToIgnore: new Set(['ignore me']),
                    },
                }
                await exportEvents?.(
                    [
                        {
                            event: 'ignore me',
                            properties: {},
                            distinct_id: 'did1',
                            team_id: 1,
                            uuid: '37114ebb-7b13-4301-b849-0d0bd4d5c7e5',
                            ip: '127.0.0.1',
                            timestamp: '2022-08-18T15:42:32.597Z',
                        },
                    ],
                    meta as any
                )

                expect(meta.global.bigQueryTable.insert).not.toHaveBeenCalled()
            })

        test('exports elements if exportElementsOnAnyEvent is true', async () => {
            const customMeta = { ...meta, config: { exportElementsOnAnyEvent: 'Yes' } }
            await exportEvents?.(
                [
                    {
                        event: 'test',
                        properties: {},
                        distinct_id: 'did1',
                        team_id: 1,
                        uuid: '37114ebb-7b13-4301-b849-0d0bd4d5c7e5',
                        ip: '127.0.0.1',
                        timestamp: '2022-08-18T15:42:32.597Z',
                        elements: [{ attr_id: 'haha' }],
                    },
                ],
                customMeta as any
            )

            expect(meta.global.bigQueryTable.insert).toHaveBeenCalledWith(
                [
                    {
                        json: {
                            bq_ingested_timestamp: expect.any(String),
                            distinct_id: 'did1',
                            event: 'test',
                            ip: '127.0.0.1',
                            properties: '{}',
                            set: '{}',
                            set_once: '{}',
                            site_url: '',
                            team_id: 1,
                            timestamp: '2022-08-18T15:42:32.597Z',
                            uuid: '37114ebb-7b13-4301-b849-0d0bd4d5c7e5',
                            elements: JSON.stringify([{ attr_id: 'haha' }])
                        },
                    },
                ],
                { createInsertId: false, partialRetries: 0, raw: true }
            )
        })
    })
})
