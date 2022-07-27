import React from 'react';
import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';
import CrossPlatformSVG from '../../static/img/cross-platform.svg'
const FeatureList = [
  {
    title: 'Low Code / No Code',
    Svg: require('../../static/img/no-code2.svg').default,
    description: (
      <>
        Starlake Data Pipeline was designed from the ground up to be easily installed and
        used to ingest your data and expose your insights quickly.
      </>
    ),
  },
  {
    title: 'On Premise and Cloud Native',
    Svg: require('../../static/img/cloud-native3.svg').default,
    description: (
      <>
        Starlake Data Pipeline support almost all On Premise and native cloud solutions,
        including         Amazon S3 / Azure Storage / Google Storage / Apache HDFS, Snowflake / Google BigQuery / Amazon Redshift / Apache Hive.
      </>
    ),
  },
  {
    title: 'DevOps friendly',
    Svg: require('../../static/img/devops.svg').default,
    description: (
      <>
          Starlake Data Pipeline was designed to fully integrate into the DevOps ecosystem to take advantage of practices like
          Git Merge Requests, incremental CI/CD, Text based configuration and BYO SQL environment.
      </>
    ),
  },
];

const textContent = {
  intro: `
React Native combines the best parts of native development with React,
a best-in-class JavaScript library for building user interfaces.
<br/><br/>
<strong>Use a little—or a lot</strong>. You can use React Native today in your existing
Android and iOS projects or you can create a whole new app from scratch.
  `,
  nativeCode: `
React primitives render to native platform UI, meaning your app uses the
same native platform APIs other apps do.
<br/><br/>
<strong>Many platforms</strong>, one React. Create platform-specific versions of components
so a single codebase can share code across platforms. With React Native,
one team can maintain two platforms and share a common technology—React.
  `,
  codeExample: `
import React from 'react';
import {Text, View} from 'react-native';
import {Header} from './Header';
import {heading} from './Typography';
const WelcomeScreen = () => (
  <View>
    <Header title="Welcome to React Native"/>
    <Text style={heading}>Step One</Text>
    <Text>
      Edit App.js to change this screen and turn it
      into your app.
    </Text>
    <Text style={heading}>See Your Changes</Text>
    <Text>
      Press Cmd + R inside the simulator to reload
      your app’s code.
    </Text>
    <Text style={heading}>Debug</Text>
    <Text>
      Press Cmd + M or Shake your device to open the
      React Native Debug Menu.
    </Text>
    <Text style={heading}>Learn</Text>
    <Text>
      Read the docs to discover what to do next:
    </Text>
   </View>
);
  `,
  forEveryone: `
React Native lets you create truly native apps and doesn't compromise your users' experiences.
It provides a core set of platform agnostic native components like <code>View</code>, <code>Text</code>, and <code>Image</code>
that map directly to the platform’s native UI building blocks.
  `,
  crossPlatform: `
React components wrap existing native code and interact with native APIs via
React’s declarative UI paradigm and JavaScript. This enables native app development
for whole new teams of developers, and can let existing native teams work much faster.
  `,
  fastRefresh: `
<strong>See your changes as soon as you save.</strong> With the power of JavaScript,
React Native lets you iterate at lightning speed. No more waiting for native builds to finish.
Save, see, repeat.
  `,
  talks: `
Members of the React Native team frequently speak at various conferences.
<br/><br/>
You can follow the latest news from the React Native team on Twitter
  `,
  videoContent:`
  has put together a short overview of React Native, where they
  explained the project in beginner's terms.
  `
};

function Section({
  element = 'section',
  children,
  className,
  background = 'light',
}) {
  const El = element;
  return <El className={`${styles.Section} ${className} ${background==="dark"? styles.dark:background==="tint"?styles.tint:""}`}>{children}</El>;
}
function Heading({text}) {
  return <h2 className={styles.Heading}>{text}</h2>;
}
function TextColumn({title, text, moreContent}) {
  return (
    <>
      <Heading text={title} />
      <div dangerouslySetInnerHTML={{__html: text}} />
      {moreContent}
    </>
  );
}

function TwoColumns({columnOne, columnTwo, reverse}) {
  return (
    <div className={`${styles.TwoColumns} ${reverse ? styles.reverse : ''}`}>
      <div className={`${styles.column} ${styles.first}  ${reverse ? styles.right : styles.left}`}>
        {columnOne}
      </div>
      <div className={`${styles.column} ${styles.last} ${reverse ? styles.left : styles.right}`}>
        {columnTwo}
      </div>
    </div>
  );
}

function CrossPlatform() {
  return (
    <Section className={styles.CrossPlatform} background="tint">
      <TwoColumns
        columnOne={
          <TextColumn
            title="Seamless Cross-Platform"
            text={textContent.crossPlatform}
          />
        }
        columnTwo={<CrossPlatformSVG />}
      />
    </Section>
  );
}
function CrossPlatform2() {
  return (
    <Section className={styles.CrossPlatform}>
      <TwoColumns
      reverse
        columnOne={
          <TextColumn
            title="Seamless Cross-Platform"
            text={textContent.crossPlatform}
          />
        }
        columnTwo={<CrossPlatformSVG />}
      />
    </Section>
  );
}
function CrossPlatform3() {
  return (
    <Section className={styles.CrossPlatform} background="tint">
      <TwoColumns
        columnOne={
          <TextColumn
            title="Seamless Cross-Platform"
            text={textContent.fastRefresh}
          />
        }
        columnTwo={<CrossPlatformSVG />}
      />
    </Section>
  );
}
function CrossPlatform4() {
  return (
    <Section className={styles.CrossPlatform}>
      <TwoColumns
      reverse
        columnOne={
          <TextColumn
            title="Seamless Cross-Platform"
            text={textContent.fastRefresh}
          />
        }
        columnTwo={<CrossPlatformSVG />}
      />
    </Section>
  );
}
function VideoContent() {
  return (
      <Section className={styles.VideoContent}>
        <br />
        <TwoColumns
          columnOne={
            <TextColumn
              title="Talks and Videos"
              text={textContent.talks}
            />
          }
          columnTwo={
            <div className={styles.vidWrapper}>
              <iframe
                src="https://www.youtube.com/embed/NCAY0HIfrwc"
                title="Mobile Innovation with React Native, ComponentKit, and Litho"
                frameBorder="0"
                allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"
                allowFullScreen
              />
            </div>
          }
        />
        <br />
        <TwoColumns
          columnOne={
            <>
              <p>
                The{' '}
                <a href="https://opensource.facebook.com/">
                  Meta Open Source team
                </a>{' '}
                {textContent.videoContent}
              </p>
            </>
          }
          columnTwo={
            <div className={styles.vidWrapper}>
              <iframe
                src="https://www.youtube.com/embed/wUDeLT6WXnQ"
                title="Explain Like I'm 5: React Native"
                frameBorder="0"
                allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"
                allowFullScreen
              />
            </div>
          }
        />
      </Section>
    
  );
}

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
          {/*<div className={styles.twoColumnsContainer}>
            <CrossPlatform/>
          <CrossPlatform2/>
          <CrossPlatform3/>
          <CrossPlatform4/>
          <VideoContent/>
          </div>*/}
    </section>
  );
}
