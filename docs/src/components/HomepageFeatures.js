import React from 'react';
import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';
import YamlSchemaValidatorSVG from '../../static/img/cross-platform.svg'
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
    yamlSchemaValidator: `
Work using your favorite VSCode <a href="https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml" target="_yaml_extension" >YAML validation extension</a> . 
Improve productivity and readability using the context sensitive entry helpers 
and intelligent YAML auto-completion feature.`,

    xlsgit: `
    Get the best of both worlds.<br> Because business users love Excel and developers love Git and text based development,
    share your ingestion metadata with business users and let them bring in any update before getting them back instantly in YAML
    for full git versioning support.
    `,
    dbReplication: `
    Select the tables and columns in your source database and replicate your data 
    into any data warehouse using full and/or incremental modes
    with optional pre and post load transformations.
    `,
    anywhere: `
    Code free ingestion of any Spark source or sink 
    including Snowflake, BigQuery, Parquet, JDBC, TEXT, XML, JSON,
    <a href="https://www.ibm.com/docs/en/order-management?topic=formats-positional-flat-files">POSITIONAL</a> sources.
    Work on any Spark distribution including Azure Synapse, Amazon EMR, Cloudera, Google Dataproc and Databricks.
    `,
    validation: `
    With advanced validation and rich metadata, define semantic types and make sure your input fields respect the specified formats.<br/>
    Mark fields as being primary/foreign keys,  optional or ignored, apply custom privacy functions and / or rename fields during the ingestion process.<br/>  
    Apply on the fly in memory transformation using any standard SQL function or custom UDF.
    `,
    security: `
    Because you take your data security seriously, Starlake makes it possible to define the access control restrictions using acccess control lists (ACL), row level security (RLS)
    and column level security (CLS).
    `,
    observability: `
    For each file ingested: get the date and time, the number of records accepted / rejected and the process duration. <br/>
    For each rejected input attribute: get the reason for rejection, the source value and the expected format.<br/>
    For each discrete column: get the list of distinct values,  modality, frequency and missing values.<br/>
    For each continuous column: get the min, the max, the mean, the median, the variance, the sum, the standard deviation, the 25th percentile and the 75th percentile.
    `,
    releditor: `
    Using the Starlake VSCode extension, create interactive entity-relationship diagrams and share them with your business users.<br/>
    Use the CLI to generate the complete entity-relationship diagram in a searchable SVG format and include it in your website.
    `,
    acleditor: `
    Using the CLI you may also generate the complete access control rules diagram in a searchable SVG format.
    `,
    byTheWay: `It's serverless.`,
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

function YamlSchemaValidator() {
  return (
    <Section className={styles.CrossPlatform} background="tint">
      <TwoColumns
        columnOne={
          <TextColumn
            title="Interactive YAML Schema Validation"
            text={textContent.yamlSchemaValidator}
          />
        }
        columnTwo={
          <img src="/starlake/img/yamlSchemaValidator2.png"/>
        }
      />
    </Section>
  );
}
function XlsGitSupport() {
  return (
    <Section className={styles.CrossPlatform} background="tint">
      <TwoColumns

        columnOne={
          <TextColumn
            title="Business & Developer Friendly"
            text={textContent.xlsgit}
          />
        }
        columnTwo={
            <img src="/starlake/img/xlsgit.png"/>
        }
      />
    </Section>
  );
}

function DbReplication() {
  return (
    <Section className={styles.CrossPlatform}>
      <TwoColumns
          reverse
        columnOne={
          <TextColumn
            title="Script Free Database Replication"
            text={textContent.dbReplication}
          />
        }
          columnTwo={
              <img src="/starlake/img/dbReplication.png"/>
          }
      />
    </Section>
  );
}

function Anywhere() {
    return (
        <Section className={styles.CrossPlatform} background="tint">
            <TwoColumns
                columnOne={
                    <TextColumn
                        title="From any source to any sink at Spark&#8482; speed."
                        text={textContent.anywhere}
                    />
                }
                columnTwo={
                    <img src="/starlake/img/anywhere.png"/>
                }
            />
        </Section>
    );
}

function Validation() {
    return (
        <Section className={styles.CrossPlatform}>
            <TwoColumns
                reverse
                columnOne={
                    <TextColumn
                        title="Keep your Lakehouse from becoming a Dataswamp."
                        text={textContent.validation}
                    />
                }
                columnTwo={
                    <img src="/starlake/img/validation.png"/>
                }
            />
        </Section>
    );
}

function Security() {
    return (
        <Section className={styles.CrossPlatform} background="tint">
            <TwoColumns

                columnOne={
                    <TextColumn
                        title="Security as a First-Class Citizen"
                        text={textContent.security}
                    />
                }
                columnTwo={
                    <img src="/starlake/img/acl.png"/>
                }
            />
        </Section>
    );
}

function Observability() {
    return (
        <Section className={styles.CrossPlatform}>
            <TwoColumns
                reverse
                columnOne={
                    <TextColumn
                        title="Data Observability through Metrics and Auditing"
                        text={textContent.observability}
                    />
                }
                columnTwo={
                    <img src="/starlake/img/observability.png"/>
                }
            />
        </Section>
    );
}

function Editor() {
    return (
        <Section className={styles.CrossPlatform}>
            <br />
            <TwoColumns
                reverse
                columnOne={
                    <TextColumn
                        title="Interactive Relationships Editor"
                        text={textContent.releditor}
                    />
                }
                columnTwo={
                    <img src="/starlake/img/relations.png"/>
                }
            />
            <br />
            <TwoColumns
                reverse
                columnOne={
                    <TextColumn
                        title=""
                        text={textContent.acleditor}
                    />                }
                columnTwo={
                    <img src="/starlake/img/aclrelations.png"/>
                }
            />
        </Section>

    );
}


function ByTheWay() {
    return (
        <Section className={styles.VideoContent}>
                    <TextColumn
                        title="And By the way ..."
                        text={textContent.byTheWay}
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
          <div className={styles.twoColumnsContainer}>
              <Anywhere/>
              <Validation/>
              <XlsGitSupport/>
              <DbReplication/>
              <Security/>
              <Observability/>
              <YamlSchemaValidator/>
              <Editor/>
              {/*



          <VideoContent/>*/}
          </div>
    </section>
  );
}
