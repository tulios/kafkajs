const RequestV4Protocol = require('./request')

describe('Protocol > Requests > Produce > v4', () => {
  let args

  beforeEach(() => {
    args = {
      acks: -1,
      timeout: 30000,
      transactionalId: null,
      compression: 0,
      topicData: [
        {
          topic: 'test-topic-5370ce2c813663fce3ca-99758-4d9ea731-5a23-4d5b-abbd-8390588d655d',
          partitions: [
            {
              partition: 0,
              firstSequence: 0,
              messages: [
                {
                  key: 'key-d5cd3b9f66c99bad43f9-99758-6869e844-8d5b-489c-a57b-db049273e7e1',
                  value:
                    'some-value-01e05f8db1f05f560a4e-99758-5522807d-67a8-4331-af74-49700cfa36f2',
                  timestamp: 1509928155660,
                  headers: {
                    'hkey-c5df4e17030b2f9da8ec-99758-fddc284f-d33e-4934-9107-f30cbf415bb7':
                      'hvalue-438ba61c75827be00c3a-99758-27c44636-d8cc-467c-b33e-feb52d46276c',
                  },
                },
                {
                  key: 'key-09f9d35672f324d3421c-99758-46c071d7-1c03-4b23-90eb-2ec6b1af864f',
                  value:
                    'some-value-96d540560a76c83ed675-99758-b23ddac7-521c-417b-8cc3-043d3b709c9f',
                  timestamp: 1509928155660,
                  headers: {
                    'hkey-48cc17a9670f774fa81f-99758-fc6b51c5-353d-41bf-8435-8966ba48bae6':
                      'hvalue-23bd6310f1af65ddbb89-99758-2e48ce3a-4f89-41a0-8f76-c76b986f2b62',
                  },
                },
                {
                  key: 'key-42558046ab47e2d1bf50-99758-f9555cd0-6e49-42cd-aa81-cb57a78d4d31',
                  value:
                    'some-value-ed896f5001b143a3405d-99758-40566a64-7f6f-45c0-b983-54e5640b6dc0',
                  timestamp: 1509928155660,
                  headers: {
                    'hkey-05e3f36cbe97c8585a17-99758-8326bcc2-c3df-43d8-9365-f82e10690ec6':
                      'hvalue-15319b33952d4e94b39f-99758-81a53e7d-164f-49d2-8de1-69533ce163b8',
                  },
                },
              ],
            },
          ],
        },
      ],
    }
  })

  describe('when acks=0', () => {
    test('expectResponse returns false', () => {
      const request = RequestV4Protocol({ ...args, acks: 0 })
      expect(request.expectResponse()).toEqual(false)
    })
  })

  test('request', async () => {
    const { buffer } = await RequestV4Protocol(args).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v4_request.json')))
  })
})
