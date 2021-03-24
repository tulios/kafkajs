/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from 'react';
import clsx from 'clsx';
import Layout from "@theme/Layout";
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import styles from './styles.module.css';

export default function Help() {
  const context = useDocusaurusContext();
  const { siteConfig = {}, language = '' } = context;

  const { customFields = {} } = siteConfig;
  const { slackUrl, repoUrl } = customFields;

  return (
    <Layout>
      <div className="hero">
        <div className="container">
          <div className="post">
            <header className="postHeader">
              <h1>Need help?</h1>
            </header>
            <p>
              This free, open-source project is developed and maintained by a small group of volunteers. Please check if your question is answered already on the {' '}
              <Link
                to={useBaseUrl('docs/faq')}
                className={clsx(styles.linkUnderlined)}
              >
                Frequently Asked Questions page
              </Link>
              {' '} or among the {' '}
              <Link
                to={repoUrl + '/issues'}
                className={clsx(styles.linkUnderlined)}
              >
                Github issues
              </Link>
              .
          </p>
            <p>
              If you're still unable to find a solution to your problem, feel free check out the links below.
          </p>
          </div>

          <section className={styles.features}>
            <div className="container">
              <div className="row">
                <div className={clsx('col col--4', styles.feature)}>
                  <h3>Browse Docs</h3>
                  <p>Learn more using the {' '}
                    <Link
                      to={useBaseUrl('docs/getting-started')}
                      className={clsx(styles.linkUnderlined)}
                    >
                      documentation on this site
                    </Link>
                  </p>
                </div>

                <div className={clsx('col col--4', styles.feature)}>
                  <h3>Join the community</h3>
                  <p>Ask questions about the documentation and project in {' '}
                    <Link
                      to={slackUrl}
                      className={clsx(styles.linkUnderlined)}
                    >
                      our Slack channel
                    </Link>
                  </p>
                </div>

                <div className={clsx('col col--4', styles.feature)}>
                  <h3>Open an issue</h3>
                  <p>Believe you have found a bug? Please {' '}
                    <Link
                      to={repoUrl + '/issues'}
                      className={clsx(styles.linkUnderlined)}
                    >
                      open an issue
                    </Link>
                    {' '} describing the issue as clearly as you can.
                  </p>
                </div>
              </div>
            </div>
          </section>
        </div>
      </div >
    </Layout >
  )
}
