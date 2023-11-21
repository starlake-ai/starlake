import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import styles from './index.module.css';

import HomepageFeatures from '@site/src/components/HomepageFeatures';

import StartSvg from '@site/static/img/start.svg';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx(styles.heroBanner)}>
      <div className="container">
        <h1 className="hero__title title_gradient">Supercharge <br/>your data pipelines</h1>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="violet_btn"
            to="/docs/intro">
            <span>Tutorial - 10min</span>
            <i>
                <StartSvg title="Start Tutorial" className="start_icon" />
                </i>
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`${siteConfig.title}`}
      description="Big & Fast Data Integration">
      <HomepageHeader />
      <HomepageFeatures />
      <a aria-label="Scroll back to top" class="clean-btn theme-back-to-top-button" type="button" href="#__docusaurus"></a>
    </Layout>
  );
}
