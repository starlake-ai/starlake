import React from 'react';
import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';

const FeatureList = [
  {
    title: 'Low Code / No Code',
    Svg: require('../../static/img/no-code2.svg').default,
    description: (
      <>
        Comet Data Pipeline was designed from the ground up to be easily installed and
        used to ingest your data and expose your insights quickly.
      </>
    ),
  },
  {
    title: 'On Premise and Cloud Native',
    Svg: require('../../static/img/cloud-native3.svg').default,
    description: (
      <>
        Comet Data Pipeline support almost all On Premise and native cloud solutions,
        including         Amazon S3 / Azure Storage / Google Storage / Apache HDFS, Snowflake / Google BigQuery / Amazon Redshift / Apache Hive.
      </>
    ),
  },
  {
    title: 'DevOps friendly',
    Svg: require('../../static/img/devops.svg').default,
    description: (
      <>
          Comet Data Pipeline was designed to fully integrate into the DevOps ecosystem to take advantage of practices like
          Git Merge Requests, incremental CI/CD, Text based configuration and BYO SQL environment.
      </>
    ),
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} alt={title} />
      </div>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
