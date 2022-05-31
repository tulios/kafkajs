const websiteUrl = require('./websiteUrl')

describe('Utils > websiteUrl', () => {
  it.each([['docs/faq'], ['/docs/faq']])('generates links to the website', path => {
    expect(websiteUrl(path)).toEqual('https://kafka.js.org/docs/faq')
  })

  it.each([
    ['why-am-i-receiving-messages-for-topics-i-m-not-subscribed-to'],
    ['#why-am-i-receiving-messages-for-topics-i-m-not-subscribed-to'],
  ])('allows specifying an anchor', anchor => {
    expect(websiteUrl('docs/faq', anchor)).toEqual(
      'https://kafka.js.org/docs/faq#why-am-i-receiving-messages-for-topics-i-m-not-subscribed-to'
    )
  })
})
