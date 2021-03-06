module.exports={
  "title": "KafkaJS",
  "tagline": "KafkaJS, a modern Apache Kafka client for Node.js",
  "url": "https://kafka.js.org",
  "baseUrl": "/",
  "organizationName": "tulios",
  "projectName": "kafkajs",
  "scripts": [
    "https://buttons.github.io/buttons.js"
  ],
  "favicon": "img/favicon.png",
  "customFields": {
    "gaGtag": true,
    "repoUrl": "https://github.com/tulios/kafkajs",
    "slackUrl": "https://kafkajs-slackin.herokuapp.com/",
    "siteConfigUrl": "https://github.com/tulios/kafkajs/edit/master/website/siteConfig.js"
  },
  "onBrokenLinks": "log",
  "onBrokenMarkdownLinks": "log",
  "presets": [
    [
      "@docusaurus/preset-classic",
      {
        "docs": {
          "homePageId": "getting-started",
          "showLastUpdateAuthor": true,
          "showLastUpdateTime": true,
          "editUrl": "https://github.com/tulios/kafkajs/edit/master/docs/",
          "path": "./docs",
          "sidebarPath": require.resolve("./sidebars.json"),
        },
        "blog": {},
        "theme": {
          "customCss": [require.resolve('./src/css/customTheme.css')],
        }
      }
    ]
  ],
  "plugins": [],
  "themeConfig": {
    "navbar": {
      "title": "KafkaJS",
      "logo": {
        "src": "img/kafkajs-logoV2.svg"
      },
      "items": [
        {
          "to": "docs/",
          "label": "Docs",
          "position": "left"
        },
        {
          "to": "/help",
          "label": "Help",
          "position": "left"
        },
        {
          "href": "https://github.com/tulios/kafkajs",
          "label": "GitHub",
          "position": "left"
        },
        {
          "label": "Version",
          "to": "docs",
          "position": "right",
          "items": [
            {
              "label": "1.15.0",
              "to": "docs/",
              "activeBaseRegex": "docs/(?!1.10.0|1.11.0|1.12.0|1.13.0|1.14.0|1.15.0|next)"
            },
            {
              "label": "1.14.0",
              "to": "docs/1.14.0/"
            },
            {
              "label": "1.13.0",
              "to": "docs/1.13.0/"
            },
            {
              "label": "1.12.0",
              "to": "docs/1.12.0/"
            },
            {
              "label": "1.11.0",
              "to": "docs/1.11.0/"
            },
            {
              "label": "1.10.0",
              "to": "docs/1.10.0/"
            },
            {
              "label": "Master/Unreleased",
              "to": "docs/next/",
              "activeBaseRegex": "docs/next/(?!support|team|resources)"
            }
          ]
        }
      ]
    },
    "image": "img/kafkajs_circle.png",
    "footer": {
      "links": [],
      "logo": {
        "src": "img/kafkajs-logoV2.svg"
      }
    },
    "algolia": {
      "apiKey": "7c56b6be30976fce32eb287e2af6cf06",
      "indexName": "kafka_js",
      "algoliaOptions": {}
    },
    "gtag": {
      "trackingID": "UA-133751873-1"
    }
  }
}