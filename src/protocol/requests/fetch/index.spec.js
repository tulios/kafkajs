const FetchVersions = require('./index')

describe('Protocol > Requests > Fetch', () => {
  for (const version of FetchVersions.versions) {
    describe(version, () => {
      const Fetch = FetchVersions.protocol({ version })
      const maxBytes = 1048576 // 1MB
      const topics = [
        {
          topic: 'test-topic',
          partitions: [
            {
              partition: 0,
              fetchOffset: 0,
              maxBytes,
            },
          ],
        },
      ]

      it('returns the requestTimeout', () => {
        const maxWaitTime = 1000
        const protocol = Fetch({
          maxWaitTime,
          minBytes: 1,
          maxBytes: 1024,
          topics,
        })

        expect(protocol.requestTimeout).toBeGreaterThan(maxWaitTime)
      })

      it('does not use numbers large than MAX_SAFE_INTEGER', () => {
        const protocol = Fetch({
          maxWaitTime: Number.MAX_SAFE_INTEGER,
          minBytes: 1,
          maxBytes: 1024,
          topics,
        })

        expect(protocol.requestTimeout).toEqual(Number.MAX_SAFE_INTEGER)
      })
    })
  }
})
