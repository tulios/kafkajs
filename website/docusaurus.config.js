module.exports = {
  "title": "KafkaJS",
  "tagline": "KafkaJS, a modern Apache Kafka client for Node.js",
  "url": "https://kafka.js.org",
  "baseUrl": "/",
  "organizationName": "tulios",
  "projectName": "kafkajs",
  "scripts": [{
    "src": "https://buttons.github.io/buttons.js",
    "async": true
  }],
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
          "to": "docs/getting-started",
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
      "style": "dark",
      "links": [
        {
          "title": "Docs",
          "items": [
            {
              "label": "Usage",
              "to": "docs/getting-started"
            }
          ]
        },
        {
          "title": "Community",
          "items": [
            {
              "label": "Slack",
              "to": "https://kafkajs-slackin.herokuapp.com"
            }
          ]
        },
        {
          "title": "More",
          "items": [
            {
              "label": "GitHub",
              "to": "https://github.com/tulios/kafkajs"
            },
            {
              "html": `
              <a
                className="github-button"
                href="https://github.com/tulios/kafkajs"
                data-icon="octicon-star"
                data-count-href="/tulios/kafkajs/stargazers"
                data-show-count="true"
                data-count-aria-label="# stargazers on GitHub"
                aria-label="Star this project on GitHub"
              >
                Star
              </a>
              `
            },
            {
              "html": `
              <a href="https://badge.fury.io/js/kafkajs">
                <img src="https://badge.fury.io/js/kafkajs.svg" alt="npm version" height="18" />
              </a>
              `
            }
          ]
        }
      ],
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