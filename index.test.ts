import { BIG_QUERY_TABLE_FIELDS, exportEvents, setupPlugin } from './index'
import { FetchError } from 'node-fetch'
import { RetryError } from '@posthog/plugin-scaffold'

const mockedBigQueryTable = {
    insert: jest.fn(),
    getMetadata: jest.fn().mockResolvedValue([{ schema: { fields: []} }]),
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

beforeEach(() => {
    console.log = jest.fn()
    console.info = jest.fn()
    console.error = jest.fn()
})

describe('BigQuery Export Plugin', () => {
    let meta: Record<string, any>

    beforeEach(() => {
        meta = {
            cache: {
                get: jest.fn(),
                set: jest.fn()
            },
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
        it('creates table if error thrown when getting metadata on a non-existent table', async () => {
            await setupPlugin?.(meta as any)
            expect(mockedDataset.createTable).not.toHaveBeenCalled()

            mockedBigQueryTable.getMetadata.mockImplementationOnce(() => {throw new Error('Not found')})
            await setupPlugin?.(meta as any)
            expect(mockedDataset.createTable).toHaveBeenCalled()
        })

        it('does no table updates if all fields already exist but not in cache', async () => {
            mockedBigQueryTable.getMetadata.mockReturnValue([{ schema: { fields: BIG_QUERY_TABLE_FIELDS as any } }])
            meta.cache.get.mockResolvedValue({
                datasetId: "1234",
                tableId: "1234",
                existingFields: BIG_QUERY_TABLE_FIELDS.length-3
            })

            await setupPlugin?.(meta as any)
            expect(mockedBigQueryTable.getMetadata).toHaveBeenCalled()
            expect(mockedBigQueryTable.setMetadata).not.toHaveBeenCalled()
            expect(mockedDataset.createTable).not.toHaveBeenCalled()
            expect(meta.cache.set).toHaveBeenCalledWith('cachedMetadata', {
                datasetId: "1234",
                tableId: "1234",
                existingFields: BIG_QUERY_TABLE_FIELDS.length
            })
        })

        it('does not call getMetadata if already in sync according to cache', async () => {
            meta.cache.get.mockResolvedValue({
                datasetId: "1234",
                tableId: "1234",
                existingFields: BIG_QUERY_TABLE_FIELDS.length
            })

            await setupPlugin?.(meta as any)
            expect(mockedBigQueryTable.getMetadata).not.toHaveBeenCalled()
            expect(mockedBigQueryTable.setMetadata).not.toHaveBeenCalled()
            expect(mockedDataset.createTable).not.toHaveBeenCalled()
            expect(meta.cache.set).not.toHaveBeenCalled()
        })

        it('updates tables if config has changed', async () => {
            mockedBigQueryTable.getMetadata.mockResolvedValue([{ schema: { fields: [] as any } }])
            meta.cache.get.mockResolvedValue({
                datasetId: "wrong",
                tableId: "1234",
                existingFields: BIG_QUERY_TABLE_FIELDS.length
            })

            await setupPlugin?.(meta as any)
            expect(mockedBigQueryTable.getMetadata).toHaveBeenCalled()
            expect(mockedBigQueryTable.setMetadata).toHaveBeenCalled()
            expect(mockedDataset.createTable).not.toHaveBeenCalled()
            expect(meta.cache.set).toHaveBeenCalledWith('cachedMetadata', {
                datasetId: "1234",
                tableId: "1234",
                existingFields: BIG_QUERY_TABLE_FIELDS.length
            })

        })

        it('throws retryError on socket errors', async () => {
            mockedBigQueryTable.getMetadata.mockRejectedValue(
                new FetchError("Client network socket disconnected before secure TLS connection was established", 'system')
            )
            meta.cache.get.mockResolvedValue({
                datasetId: "wrong",
                tableId: "1234",
                existingFields: BIG_QUERY_TABLE_FIELDS.length
            })

            expect(async () => await setupPlugin?.(meta as any)).rejects.toThrow(RetryError)

        })
    })

    describe('exportEvents()', () => {
        it('makes the right call to bigQueryTable.insert', async () => {
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

        it('ignores events in exportEventsToIgnore', async () => {
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

        it('exports elements if exportElementsOnAnyEvent is true', async () => {
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

        describe('error handling', () => {
            const events = [
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
            ]
            const ENTITY_TOO_LARGE_ERROR = "Multiple errors occurred during the request. Please see the `errors` array for complete details.\n\n 1. Request Entity Too Large\n 2. <!DOCTYPE html>\n<html lang=en>\n <meta charset=utf-8>\n <meta name=viewport content=\"initial-scale=1, minimum-scale=1, width=device-width\">\n <title>Error 413 (Request Entity Too Large)!!1</title>\n <style>\n *{margin:0;padding:0}html,code{font:15px/22px arial,sans-serif}html{background:#fff;color:#222;padding:15px}body{margin:7% auto 0;max-width:390px;min-height:180px;padding:30px 0 15px}* > body{background:url(//www.google.com/images/errors/robot.png) 100% 5px no-repeat;padding-right:205px}p{margin:11px 0 22px;overflow:hidden}ins{color:#777;text-decoration:none}a img{border:0}@media screen and (max-width:772px){body{background:none;margin-top:0;max-width:none;padding-right:0}}#logo{background:url(//www.google.com/images/branding/googlelogo/1x/googlelogo_color_150x54dp.png) no-repeat;margin-left:-5px}@media only scree"

            it('raises a RetryError if insert failed', async () => {
                mockedBigQueryTable.insert.mockRejectedValueOnce(new Error("Some BigQuery error"))

                const promise = exportEvents?.(events, meta as any)

                await expect(promise).rejects.toEqual(new RetryError('Error inserting into BigQuery! Some BigQuery error'))
            })

            it('handles Request Entity Too Large errors', async () => {
                mockedBigQueryTable.insert.mockRejectedValueOnce(new Error(ENTITY_TOO_LARGE_ERROR))

                await exportEvents?.(events, meta as any)

                expect(mockedBigQueryTable.insert).toHaveBeenCalledTimes(3)
                expect(meta.global.bigQueryTable.insert.mock.calls[1][0]).toEqual(
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
                            }
                        }
                    ]
                )

                expect(meta.global.bigQueryTable.insert.mock.calls[2][0]).toEqual(
                    [
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
                            }
                        }
                    ]
                )
            })

            it('stops retrying on Request Entity Too Large when only a single event', async () => {
                mockedBigQueryTable.insert.mockRejectedValueOnce(
                    new Error(ENTITY_TOO_LARGE_ERROR)
                )

                const promise = exportEvents?.([events[0]], meta as any)

                await expect(promise).rejects.toEqual(new Error(`Error inserting into BigQuery! ${ENTITY_TOO_LARGE_ERROR}`))
            })
        })
    })
})
