const produceRequest = require('./produceRequest')

describe('Messages - produceRequest', () => {
  it('works', () => {
    const output = produceRequest({
      acks: 0,
      timeout: 1000,
      topicData: [
        {
          topic: 'topic-1',
          data: [
            {
              partition: 0,
              recordSet: [
                {
                  key: 1,
                  value: JSON.stringify({ name: 'test' }),
                },
              ],
            },
          ],
        },
      ],
    })
    console.log(output)
  })
})
