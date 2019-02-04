/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// See https://docusaurus.io/docs/site-config for all the possible
// site configuration options.

const repoUrl = 'https://github.com/tulios/kafkajs'
const slackUrl = 'https://kafkajs-slackin.herokuapp.com/'

const siteConfig = {
  title: 'KafkaJS',
  tagline: 'A modern Apache Kafka client for Node.js',
  url: 'https://kafka.js.org', // Your website URL
  baseUrl: '/',
  projectName: 'kafkajs',
  organizationName: 'tulios',
  cname: 'kafka.js.org',
  headerLinks: [
    { doc: 'getting-started', label: 'Docs' },
    { page: 'help', label: 'Help' },
    { href: repoUrl, label: 'GitHub' },
  ],
  headerIcon: 'img/kafkajs-logo.svg',
  footerIcon: 'img/kafkajs-logo.svg',
  favicon: 'img/favicon.png',
  colors: {
    primaryColor: '#24292e',
    secondaryColor: '#2f363d',
    linkColor: '#0366d6',
  },
  /* Custom fonts for website */
  /*
  fonts: {
    myFont: [
      "Times New Roman",
      "Serif"
    ],
    myOtherFont: [
      "-apple-system",
      "system-ui"
    ]
  },
  */
  highlight: {
    theme: 'github',
  },

  // Add custom scripts here that would be placed in <script> tags.
  scripts: ['https://buttons.github.io/buttons.js'],
  // On page navigation for the current documentation page.
  onPageNav: 'separate',
  // No .html extensions for paths.
  cleanUrl: true,
  editUrl: repoUrl + '/edit/master/docs/',

  // Open Graph and Twitter card images.
  ogImage: 'img/docusaurus.png',
  twitterImage: 'img/docusaurus.png',

  gaTrackingId: 'UA-133751873-1',
  gaGtag: true,

  // You may provide arbitrary config keys to be used as needed by your
  // template.
  repoUrl,
  slackUrl,
  siteConfigUrl: repoUrl + '/edit/master/website/siteConfig.js',
}

module.exports = siteConfig
