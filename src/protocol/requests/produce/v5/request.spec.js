const RequestV5Protocol = require('./request')

describe('Protocol > Requests > Produce > v5', () => {
  let args

  beforeEach(() => {
    args = {
      acks: -1,
      timeout: 30000,
      transactionalId: null,
      compression: 0,
      topicData: [
        {
          topic: 'test-topic-1c8ace0ecfb3cb281243-706-b9f24ac1-6a1e-4458-ba5f-5fc0c51a46c7',
          partitions: [
            {
              partition: 0,
              firstSequence: 0,
              messages: [
                {
                  key: 'key-d319075933d888e408bc-706-fbbed489-92e5-4f02-b230-014dd3784e5a',
                  value: 'some-value-bbf65807336003358c44-706-13350820-511c-48f1-a6df-f958e456912c',
                  timestamp: 1509928155660,
                  headers: {
                    'hkey-ca71207e127ca2c70c05-706-b08da511-56e6-453b-b16e-3ec022b8dd6f':
                      'hvalue-bbdb46905198e3e37cc9-706-cfc42cd5-8a54-4c89-b7d8-c8455c4ae7dc',
                  },
                },
                {
                  key: 'key-788fdfca430de5811b96-706-7f435dff-6fed-43ba-a354-e2390382f67c',
                  value: 'some-value-358f20a7530d8828012c-706-6e4c9495-bc6f-4a31-8132-eef704fa6200',
                  timestamp: 1509928155660,
                  headers: {
                    'hkey-779dac3340e486209484-706-82e9df9e-a10d-44fc-b87c-cc9d38396bd6':
                      'hvalue-d02355b58de96ff30821-706-3e724986-a6db-4c35-9759-c53ce82e7e8e',
                  },
                },
                {
                  key: 'key-86b4648e49189cf59008-706-ac4eb7b8-430c-4798-88a4-5ebd73069b2d',
                  value: 'some-value-54e08078b0463df8fab9-706-eec3991d-c68d-45e6-953c-8575254a3f33',
                  timestamp: 1509928155660,
                  headers: {
                    'hkey-a977d0fd09fdea0fa1bb-706-979dbc08-c04b-4b7b-99d8-449373521ba8':
                      'hvalue-57152eb6f8719e706fdc-706-ebf22d1f-1379-4b1f-bfa8-adbd24064cd4',
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
      const request = RequestV5Protocol({ ...args, acks: 0 })
      expect(request.expectResponse()).toEqual(false)
    })
  })

  test('request', async () => {
    const { buffer } = await RequestV5Protocol(args).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v5_request.json')))
  })
})
