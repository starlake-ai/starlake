import React from 'react';
import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';

import ReactPlayer from 'react-player/lazy';

const FeatureList = [
  {
    title: 'Low Code / No Code',
    Svg: require('@site/static/img/lowcode.svg').default,
    description: (
      <>
        Starlake Data Pipeline was designed from the ground up to be easily installed and used to ingest your data and expose your insights quickly.
      </>
    ),
  },
  {
    title: (
        <>
            On Premise<br/> & Cloud Native
        </>
    ),
    Svg: require('@site/static/img/cloudnative.svg').default,
    description: (
      <>
        Starlake Data Pipeline support almost all On Premise and native cloud solutions, including Amazon S3 / Azure Storage / Google Storage / Apache HDFS, Snowflake / Google BigQuery / Amazon Redshift / Apache Hive.
      </>
    ),
  },
  {
    title: 'DevOps friendly',
    Svg: require('@site/static/img/devops.svg').default,
    description: (
      <>
        Starlake Data Pipeline was designed to fully integrate into the DevOps ecosystem to take advantage of practices like Git Merge Requests, incremental CI/CD, Text based configuration and BYO SQL environment.
      </>
    ),
  },
];

/* alignImg : left/right/center/full/big  */
const SplitSectionList = [
  {
      title: (
          <>
          From any source <br/>to any sink at <br/>native speed.
          </>
      ),
      isBackgroundColored: 'yes',
      reverse: 'no',
      Img: require('@site/static/img/spark_graph_new.svg').default,
      ImgMobile: require('@site/static/img/spark_graph_new.svg').default,
      alignImg: 'right',
      description: (
       <p>
       </p>
      ),
      linkText: 'Discover',
      linkURL: 'docs/userguide/infer-schema',
      LinkSvg: require('@site/static/img/eye.svg').default,
  },
  {
      title: (
          <>
          Keep your <br/>Lakehouse from <br/>becoming a <br/>Dataswamp.
          </>
      ),
      isBackgroundColored: 'yes',
      reverse: 'yes',
      Img: require('@site/static/img/lakehouse.svg').default,
      ImgMobile: require('@site/static/img/lakehouse_mobile.svg').default,
      alignImg: 'full',
      linkText: 'Read more',
      linkURL: '#',
      LinkSvg: require('@site/static/img/arrow_right.svg').default,
  },
  {
      title: (
          <>
          Business & <br/>Developer Friendly
          </>
      ),
      isBackgroundColored: 'no',
      reverse: 'no',
      Img: require('@site/static/img/devfriendly.svg').default,
      ImgMobile: require('@site/static/img/devfriendly.svg').default,
      alignImg: 'left',
      description:(
        <p>
          share your data catalog with your business users and let them contribute and explore with their favorite tools.
        </p>
      ),
      linkText: 'Read more',
      linkURL: '#',
      LinkSvg: require('@site/static/img/arrow_right.svg').default,
  },
  {
      title: (
          <>
          Script Free <br/>Database <br/>Replication
          </>
      ),
      isBackgroundColored: 'yes',
      reverse: 'yes',
      Img: require('@site/static/img/scriptfree.svg').default,
      ImgMobile: require('@site/static/img/scriptfree_mobile.svg').default,
      alignImg: 'full',
      BeforeTitle: require('@site/static/img/scriptfree_icon.svg').default,
      description:(
        <p>
          Automated full and incremental replication of your data from any relational database to your favorite datawarehouse without writing a single line of code.
        </p>
      ),
      linkText: 'Read more',
      linkURL: '#',
      LinkSvg: require('@site/static/img/arrow_right.svg').default,
  },
  {
      title: (
          <>
          Security as <br/>a First-Class <br/>Citizen
          </>
      ),
      isBackgroundColored: 'yes',
      reverse: 'no',
      Img: require('@site/static/img/security_new.svg').default,
      ImgMobile: require('@site/static/img/security_new.svg').default,
      alignImg: 'full',
      BeforeTitle: require('@site/static/img/security_icon.svg').default,
      linkText: 'Read more',
      linkURL: '#',
      LinkSvg: require('@site/static/img/arrow_right.svg').default,
  },
  {
      title: (
          <>
          Data Observability <br/>through Metrics <br/>and Auditing
          </>
      ),
      isBackgroundColored: 'no',
      reverse: 'yes',
      Img: require('@site/static/img/observability.svg').default,
      ImgMobile: require('@site/static/img/observability.svg').default,
      alignImg: 'full',
      description:(
        <p>
          Pellentesque sagittis dictum ex, quis maximus purus fermentum sit amet.
        </p>
      ),
      linkText: 'Read more',
      linkURL: '#',
      LinkSvg: require('@site/static/img/arrow_right.svg').default,
  },
  {
      title: (
          <>
          Interactive YAML <br/>Schema Validation
          </>
      ),
      isBackgroundColored: 'yes',
      reverse: 'no',
      video:'https://www.youtube.com/watch?v=ScMzIvxBSi4',
      alignImg: 'big',
      description:(
        <p>
          Donec quis vestibulum odio. Quisque ultrices eros diam, vel dignissim orci sollicitudin vel.
        </p>
      ),
      linkText: 'Read more',
      linkURL: '#',
      LinkSvg: require('@site/static/img/arrow_right.svg').default,
  },
  {
      title: (
          <>
          Interactive <br/>Relationships <br/>Editor
          </>
      ),
      isBackgroundColored: 'no',
      reverse: 'yes',
      Img: require('@site/static/img/relationshipeditor_new.svg').default,
      ImgMobile: require('@site/static/img/relationshipeditor_new.svg').default,
      alignImg: 'left',
      description:(
        <p>
          Etiam ac tellus a ex sollicitudin gravida ut ut sapien. Phasellus sed felis consectetur, lacinia lectus id, sollicitudin mauris.
        </p>
      ),
      linkText: 'Read more',
      linkURL: '#',
      LinkSvg: require('@site/static/img/arrow_right.svg').default,
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4 feature')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

function SplitSection({isBackgroundColored, reverse, Img, ImgMobile, alignImg, video, BeforeTitle, title, description, linkText, linkURL, LinkSvg}) {
    return(
        <section className={`${clsx('split_section')} ${isBackgroundColored=='yes' ? "coloredBckg" : ''}`}>
          <div className="container">
            <div className={`${clsx('row align-items-center')} ${reverse=='yes' ? styles.reverse : ''}`}>
                <div className={`${alignImg=='big' ? clsx('col col--4') : clsx('col col--6')}`}>
                    <div className={styles.split_content}>
                        {BeforeTitle ? <BeforeTitle className={styles.beforeTitle} role="img" /> : ""}
                        <h2 className="title_gradient">{title}</h2>
                        {description}
                        <a title={linkText} href={linkURL} className="violet_btn">
                            <span>{linkText}</span>
                            <i>
                                <LinkSvg className={styles.link_icon} role="img" />
                            </i>
                        </a>
                    </div>
                </div>
                <div className={`${alignImg=='big' ? clsx('col col--8') : clsx('col col--6')}`}>
                    <div className={`${styles.split_img} align_${alignImg} ${video ? clsx('is_video') : ''}`}>
                        {Img ? <Img className="img-fluid hide_mobile" role="img" /> : '' }
                        {ImgMobile ? <ImgMobile className="img-fluid hide_desktop" role="img" /> : ""}                     
                        {video ? <ReactPlayer url={video} /> : ""}                        
                    </div>
                </div>
            </div>
           </div>
        </section>
    );
}

export default function HomepageFeatures() {
  return (
      <main>
        <section className={styles.features}>
          <div className="container">
            <div className="row">
              {FeatureList.map((props, idx) => (
                <Feature key={idx} {...props} />
              ))}
            </div>
          </div>
        </section>
        {SplitSectionList.map((props, idx) => (
            <SplitSection key={idx} {...props} />
        ))}
      </main>
  );
}
