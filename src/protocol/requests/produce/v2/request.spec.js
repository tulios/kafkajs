const os = require('os')
const RequestV2Protocol = require('./request')
const { Types } = require('../../../message/compression')

const osType = os.type().toLowerCase()

describe('Protocol > Requests > Produce > v2', () => {
  let args

  beforeEach(() => {
    args = {
      acks: -1,
      timeout: 30000,
      compression: 0,
      topicData: [
        {
          topic: 'test-topic-9f825c3f60bb0b4db583',
          partitions: [
            {
              partition: 0,
              messages: [
                {
                  key: 'key-bb252ae5801883c12bbd',
                  value: 'some-value-10340c6329f8bbf5b4a2',
                  timestamp: 1509819296569,
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
      const request = RequestV2Protocol({ ...args, acks: 0 })
      expect(request.expectResponse()).toEqual(false)
    })
  })

  test('request', async () => {
    const { buffer } = await RequestV2Protocol({
      acks: -1,
      timeout: 30000,
      compression: 0,
      topicData: [
        {
          topic: 'test-topic-9f825c3f60bb0b4db583',
          partitions: [
            {
              partition: 0,
              messages: [
                {
                  key: 'key-bb252ae5801883c12bbd',
                  value: 'some-value-10340c6329f8bbf5b4a2',
                  timestamp: 1509819296569,
                },
                {
                  key: 'key-8a14e73a88e93f7c3a39',
                  value: 'some-value-4fa91513bffbcc0e34b3',
                  timestamp: 1509819296569,
                },
                {
                  key: 'key-183a2d8eb3683f080b82',
                  value: 'some-value-938afcf1f2ef0439c752',
                  timestamp: 1509819296569,
                },
              ],
            },
          ],
        },
      ],
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v2_request.json')))
  })

  test('request with gzip', async () => {
    const { buffer } = await RequestV2Protocol({
      acks: -1,
      timeout: 30000,
      compression: Types.GZIP,
      topicData: [
        {
          topic: 'test-topic-43395f618a885920238c',
          partitions: [
            {
              partition: 0,
              messages: [
                {
                  key: 'key-d27f2271f5447fe62503',
                  value: 'some-value-e64a333e986853959623',
                  timestamp: 1509928155660,
                },
                {
                  key: 'key-3be6f0b8e6c987d0aedc',
                  value: 'some-value-7259046cfda805b0172e',
                  timestamp: 1509928155660,
                },
                {
                  key: 'key-af98821b43a80d6aa4e8',
                  value: 'some-value-94b9e769ec3e401bfd57',
                  timestamp: 1509928155660,
                },
              ],
            },
          ],
        },
      ],
    }).encode()
    expect(buffer).toEqual(Buffer.from(require(`../fixtures/v2_request_gzip_${osType}.json`)))
  })
})
