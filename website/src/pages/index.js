/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './styles.module.css';

const features = [
  {
    title: 'No Dependencies',
    content: 'Committed to staying lean and dependency free. 100% Javascript, with no native addons required.',
  },
  {
    title: 'Well Tested',
    content: 'Every commit is tested against a production-like multi-broker Kafka cluster, ensuring that regressions never make it into production.',
  },
  {
    title: 'Battle Hardened',
    content: 'Dog-fooded by the authors in dozens of high-traffic services with strict uptime requirements.',
  },
]

function Feature({ title, content }) {
  return (
    <div className={clsx('col col--4', styles.feature)}>
      <h3>{title}</h3>
      <p>{content}</p>
    </div>
  )
}

export default function Index() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  return (
    <Layout
      title={`${siteConfig.title} ${siteConfig.titleDelimiter} ${siteConfig.tagline}`}
    >

      <div className={clsx('hero hero--dark', styles.heroBanner)}>
        <div className="container">
          <img
            className={clsx(styles.heroBannerLogo)}
            alt="KafkaJS logo"
            src={useBaseUrl('img/kafkajs-logoV2.svg')}
          />
          <p className={clsx(styles.heroSubtitle)}>{siteConfig.tagline}</p>

          <div className={styles.getStarted}>
            <Link
              className="button button--outline button--md"
              to={useBaseUrl('docs/getting-started')}
            >
              Documentation
            </Link>

            <Link
              className="button button--outline button--md"
              to={siteConfig.customFields.repoUrl}
            >
              GitHub
            </Link>
          </div>

          <div className={clsx(styles.heroTrademark, "hero__subtitle")}>
            <small>
            KAFKA is a registered trademark of The Apache Software Foundation and has been licensed for use by KafkaJS. KafkaJS has no affiliation with and is not endorsed by The Apache Software Foundation.
            </small>
          </div>
        </div>
      </div>

      <main>
        {features && features.length > 0 && (
          <section className={styles.features}>
            <div className="container">
              <div className="row">
                {features.map((props, idx) => (
                  <Feature key={idx} {...props} />
                ))}
              </div>
            </div>
          </section>
        )}
      </main>
    </Layout>
  )
}
