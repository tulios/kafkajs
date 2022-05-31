---
id: version-1.11.0-pre-releases
title: Pre-releases
original_id: pre-releases
---

Stable KafkaJS versions can take a while to be released. Release candidates are usually deployed in production for at least a week before general availability. We do this to make sure that all releases are stable, and we trust our services to verify most production use-cases. Although this process guarantees some quality, it can be too slow or unpredictable. Some versions go out faster than others, and it can be frustrating if you need a particular feature that has not been included in a stable release yet.

In the past, the recommendation was to point your `package.json` to the commit hash with the change you needed, but this approach has some complications. Companies usually proxy NPM and have some expectations on how dependencies are handled, your CI or deploy server might not have access to GitHub, among other issues. This process was an extra barrier for users to try out the new code, helping us test and perfect the next release.

To alleviate this issue, we are now automatically creating pre-release versions on every merge to master containing changes to `src/`, `index.js` or, `package.json`.

## Using pre-release versions

To use the latest pre-release version, run:

```sh
# Yarn
yarn add kafkajs@beta

# Npm
npm install --save kafkajs@beta
```

## Versioning

If the current stable version is `1.9.3` and we merge a new PR to master, a pre-release will be generated with the version `1.10.0-beta.0`. It will always use the next minor version. If a second PR is merged, the next version would be `1.10.0-beta.1`, since we are generating a new pre-release for the same stable version, it will continue incrementing the last number. If we release a patch, it will continue to make beta releases using the next minor, so stable on `1.9.4` will continue to generate `1.10.0-beta.N`.

## Pre-release package changes

In order to know what a pre-release version contains, we have added a new attribute to the `package.json` of pre-release versions, `kafkajs`:

```javascript
{
  // package.json
  "kafkajs": {
    "sha": "43e325e18133b8d6c1c80f8e95ef8610c44ec631",
    "compare": "https://github.com/tulios/kafkajs/compare/v1.9.3...43e325e18133b8d6c1c80f8e95ef8610c44ec631"
  }
}
```

This contains the git SHA used to generate the version and the GitHub URL to quickly compare the differences with the stable version at the time of the pre-release creation.
