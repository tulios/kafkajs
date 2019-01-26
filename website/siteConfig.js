/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// See https://docusaurus.io/docs/site-config for all the possible
// site configuration options.

const repoUrl = 'https://github.com/tulios/kafkajs'

const siteConfig = {
  title: 'KafkaJS',
  tagline: 'A modern Apache Kafka client for Node.js',
  url: 'https://kafkajs.github.io', // Your website URL
  baseUrl: '/',
  projectName: 'kafkajs',
  organizationName: 'kafkajs',
  headerLinks: [{ doc: 'getting-started', label: 'Docs' }, { page: 'help', label: 'Help' }],
  headerIcon: 'img/docusaurus.svg',
  footerIcon: 'img/docusaurus.svg',
  favicon: 'img/favicon.png',
  colors: {
    primaryColor: '#2E8555',
    secondaryColor: '#205C3B',
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
    theme: 'default',
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

  // You may provide arbitrary config keys to be used as needed by your
  // template.
  repoUrl,
  siteConfigUrl: repoUrl + '/edit/master/website/siteConfig.js',
}

module.exports = siteConfig
