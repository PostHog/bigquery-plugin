import { exportEventsToBigQuery } from './index'

test("exportEventsToBigQuery()", async () => {
    const meta = {
        config: {
            exportElementsOnAnyEvent: 'No'
        },
        global: {
            bigQueryTable: {
                insert: jest.fn()
            }
        }
    }
    await exportEventsToBigQuery([
        {
            event: 'test',
            properties: {},
            distinct_id: 'did1',
            team_id: 1,
            uuid: '37114ebb-7b13-4301-b849-0d0bd4d5c7e5',
            ip: '127.0.0.1',
            timestamp: '2022-08-18T15:42:32.597Z',
        }
    ], meta as any)
    expect(meta.global.bigQueryTable.insert).toHaveBeenCalledWith(
        [
            { 
                "json": { 
                    "bq_ingested_timestamp": expect.any(String), 
                    "distinct_id": "did1", 
                    "elements": "{}", 
                    "event": "test", 
                    "ip": "127.0.0.1", 
                    "properties": "{}", 
                    "set": "{}", 
                    "set_once": "{}", 
                    "team_id": 1, 
                    "timestamp": '2022-08-18T15:42:32.597Z', 
                    "uuid": "37114ebb-7b13-4301-b849-0d0bd4d5c7e5" 
                } 
            }
        ], 
        { 
            "createInsertId": false, 
            "partialRetries": 0, 
            "raw": true 
        }
    )
})