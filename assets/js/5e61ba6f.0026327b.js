"use strict";(self.webpackChunkstarlake=self.webpackChunkstarlake||[]).push([[3524],{3905:(t,e,a)=>{a.d(e,{Zo:()=>s,kt:()=>g});var n=a(67294);function r(t,e,a){return e in t?Object.defineProperty(t,e,{value:a,enumerable:!0,configurable:!0,writable:!0}):t[e]=a,t}function l(t,e){var a=Object.keys(t);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(t);e&&(n=n.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),a.push.apply(a,n)}return a}function i(t){for(var e=1;e<arguments.length;e++){var a=null!=arguments[e]?arguments[e]:{};e%2?l(Object(a),!0).forEach((function(e){r(t,e,a[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(a)):l(Object(a)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(a,e))}))}return t}function o(t,e){if(null==t)return{};var a,n,r=function(t,e){if(null==t)return{};var a,n,r={},l=Object.keys(t);for(n=0;n<l.length;n++)a=l[n],e.indexOf(a)>=0||(r[a]=t[a]);return r}(t,e);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(t);for(n=0;n<l.length;n++)a=l[n],e.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(t,a)&&(r[a]=t[a])}return r}var d=n.createContext({}),p=function(t){var e=n.useContext(d),a=e;return t&&(a="function"==typeof t?t(e):i(i({},e),t)),a},s=function(t){var e=p(t.components);return n.createElement(d.Provider,{value:e},t.children)},u="mdxType",m={inlineCode:"code",wrapper:function(t){var e=t.children;return n.createElement(n.Fragment,{},e)}},k=n.forwardRef((function(t,e){var a=t.components,r=t.mdxType,l=t.originalType,d=t.parentName,s=o(t,["components","mdxType","originalType","parentName"]),u=p(a),k=r,g=u["".concat(d,".").concat(k)]||u[k]||m[k]||l;return a?n.createElement(g,i(i({ref:e},s),{},{components:a})):n.createElement(g,i({ref:e},s))}));function g(t,e){var a=arguments,r=e&&e.mdxType;if("string"==typeof t||r){var l=a.length,i=new Array(l);i[0]=k;var o={};for(var d in e)hasOwnProperty.call(e,d)&&(o[d]=e[d]);o.originalType=t,o[u]="string"==typeof t?t:r,i[1]=o;for(var p=2;p<l;p++)i[p]=a[p];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}k.displayName="MDXCreateElement"},49605:(t,e,a)=>{a.r(e),a.d(e,{assets:()=>d,contentTitle:()=>i,default:()=>m,frontMatter:()=>l,metadata:()=>o,toc:()=>p});var n=a(87462),r=(a(67294),a(3905));const l={sidebar_position:1},i="What is Starlake ?",o={unversionedId:"intro",id:"version-1.0.0/intro",title:"What is Starlake ?",description:"Starlake is a configuration only Extract, Load and Transform engine.",source:"@site/versioned_docs/version-1.0.0/0000-intro.md",sourceDirName:".",slug:"/intro",permalink:"/starlake/docs/1.0.0/intro",draft:!1,editUrl:"https://github.com/starlake-ai/starlake/edit/master/docs/versioned_docs/version-1.0.0/0000-intro.md",tags:[],version:"1.0.0",sidebarPosition:1,frontMatter:{sidebar_position:1},sidebar:"starlakeSidebar",next:{title:"Concepts",permalink:"/starlake/docs/1.0.0/category/concepts"}},d={},p=[{value:"Data Lifecycle",id:"data-lifecycle",level:2},{value:"Data Extraction",id:"data-extraction",level:2},{value:"Data Loading",id:"data-loading",level:2},{value:"Data Transformation",id:"data-transformation",level:2},{value:"Illustration",id:"illustration",level:2}],s={toc:p},u="wrapper";function m(t){let{components:e,...l}=t;return(0,r.kt)(u,(0,n.Z)({},s,l,{components:e,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"what-is-starlake-"},"What is Starlake ?"),(0,r.kt)("p",null,"Starlake is a configuration only Extract, Load and Transform engine.\nThe workflow below is a typical use case:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Extract your data as a set of Fixed Position, DSV (Delimiter-separated values) or JSON or XML files"),(0,r.kt)("li",{parentName:"ul"},"Define or infer the structure of each POSITION/DSV/JSON/XML file with a schema using YAML syntax"),(0,r.kt)("li",{parentName:"ul"},"Configure the loading process"),(0,r.kt)("li",{parentName:"ul"},"Start watching your data being available as Tables in your warehouse."),(0,r.kt)("li",{parentName:"ul"},"Build aggregates using SQL, Jinja and YAML configuration files.  ")),(0,r.kt)("p",null,"Starlake may be used indistinctly for all or any of these steps."),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"The ",(0,r.kt)("inlineCode",{parentName:"li"},"extract")," step allows to export selective data from an existing SQL database to a set of CSV files."),(0,r.kt)("li",{parentName:"ul"},"The ",(0,r.kt)("inlineCode",{parentName:"li"},"load")," step allows you to load text files, to ingest FIXED-WIDTH/CSV/JSON/XML files as strong typed records stored as parquet files or DWH tables (eq. Google BigQuery) or whatever sink you configured"),(0,r.kt)("li",{parentName:"ul"},"The ",(0,r.kt)("inlineCode",{parentName:"li"},"transform")," step allows to join loaded data and save them as parquet files, DWH tables or Elasticsearch indices")),(0,r.kt)("p",null,"The Load and Transform steps support multiple configurations for inputs and outputs as illustrated in the\nfigure below. "),(0,r.kt)("p",null,(0,r.kt)("img",{alt:"Anywhere",src:a(8457).Z,title:"Anywhere",width:"1100",height:"929"})),(0,r.kt)("h2",{id:"data-lifecycle"},"Data Lifecycle"),(0,r.kt)("p",null,"The figure below illustrates the typical data lifecycle in Starlake.\n",(0,r.kt)("img",{src:a(67973).Z,width:"1467",height:"960"})),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Landing Area: In this optional step, files with predefined filename patterns are stored on a local filesystem in a predefined folder hierarchy\n",(0,r.kt)("em",{parentName:"li"},"-")," Pending Area: Files associated with a schema are imported into this area."),(0,r.kt)("li",{parentName:"ul"},"Accepted Area: Pending files are parsed against their schema and records are rejected or accepted and made available in  Bigquery/Snowflake/Databricks/Hive/... tables or parquet files in a cloud bucket."),(0,r.kt)("li",{parentName:"ul"},"Business Area: Tables (Hive / BigQuery / Parquet files / ...) in the working area may be joined to provide a holistic view of the data through the definition of transformations."),(0,r.kt)("li",{parentName:"ul"},"Data visualization: parquet files / tables may be exposed in data warehouses or elasticsearch indices through an indexing definition")),(0,r.kt)("p",null,"Input file schemas, ingestion rules, transformation and indexing definitions used in the steps above are all defined in YAML files."),(0,r.kt)("h2",{id:"data-extraction"},"Data Extraction"),(0,r.kt)("p",null,"Starlake provides a fast way to extract, in full or incrementally, tables from your database. "),(0,r.kt)("p",null,"Using parallel load through a JDBC connection and configuring the incremental fields in the schema, you may extract your data incrementally.\nOnce copied to the cloud provider of your choice, the data is available for further processing by the Load and Transform steps."),(0,r.kt)("p",null,"The extraction module support two modes:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Native mode: Native database scripts are generated and must be run against your database."),(0,r.kt)("li",{parentName:"ul"},"JDBC mode: In this mode, Starlake will spawn a number of threads to extract the data. We were able to extract an average of 1 million records per second using the AdventureWorks database on Postgres.")),(0,r.kt)("h2",{id:"data-loading"},"Data Loading"),(0,r.kt)("p",null,"Usually, data loading is done by writing hand made custom parsers that transform input files into datasets of records."),(0,r.kt)("p",null,"Starlake aims at automating this parsing task by making data loading purely declarative."),(0,r.kt)("p",null,"The major benefits the Starlake data loader bring to your warehouse are:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Eliminates manual coding for data loading"),(0,r.kt)("li",{parentName:"ul"},"Assign metadata to each dataset"),(0,r.kt)("li",{parentName:"ul"},"Expose data loading metrics and history"),(0,r.kt)("li",{parentName:"ul"},"Transform text files to strongly typed records without coding"),(0,r.kt)("li",{parentName:"ul"},"Support semantic types by allowing you to set type constraints on the incoming data"),(0,r.kt)("li",{parentName:"ul"},"Apply privacy to specific fields"),(0,r.kt)("li",{parentName:"ul"},"Apply security at load time"),(0,r.kt)("li",{parentName:"ul"},"Preview your data lifecycle and publish in SVG format"),(0,r.kt)("li",{parentName:"ul"},"Support multiple data sources and sinks"),(0,r.kt)("li",{parentName:"ul"},"Starlake is a very, very simple piece of software to administer")),(0,r.kt)("p",null,"The Load module supports 2 modes:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"The native mode, the fatest one will use the target database capabilities to load the data and apply all the required transformations. For example, on BigQuery, starlake use the Bigquery Load API"),(0,r.kt)("li",{parentName:"ul"},"The Spark mode will use Spark to load the data. This mode is slower than the native mode but is the most powerful one and is compatible with all databases. Please note that this mode does not require setting up a Spark cluster, it run out of the box in the starlake docker image.")),(0,r.kt)("p",null,"The table below list the features supported by each mode, the one that meet your requirements depend on the quality of your source and on the audit level you wish to have:"),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"File formats"),(0,r.kt)("th",{parentName:"tr",align:null},"Spark"),(0,r.kt)("th",{parentName:"tr",align:null},"Native"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"CSV"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"CSV with multichar separator"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"JSON Lines"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"JSON Array"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"XML"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"POSITION"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Parquet"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Avro"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Kafka"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Any JDBC database"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Any Spark Source"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Any char encoding including chinese, arabic, celtic ..."),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null})))),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Error Handling Levels"),(0,r.kt)("th",{parentName:"tr",align:null},"Spark"),(0,r.kt)("th",{parentName:"tr",align:null},"Native"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"File level"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Column level reporting"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Produce replay file"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Handle unlimited number of errors"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null})))),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Features"),(0,r.kt)("th",{parentName:"tr",align:null},"Spark"),(0,r.kt)("th",{parentName:"tr",align:null},"Native"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"rename domain"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"rename table"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"rename fields"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"add new field (scripted fields)"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Apply privacy on field (transformation)"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Validate type"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Validate pattern (semantic types)"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Ignore fields"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"remove fields after transformation"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"date and time fields any format"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"-")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"keep filename"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Partition table"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Append Mode"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Overwrite Mode"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Merge Mode"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Pre/Post SQL"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Apply assertions"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null})))),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Security"),(0,r.kt)("th",{parentName:"tr",align:null},"Spark"),(0,r.kt)("th",{parentName:"tr",align:null},"Native"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Apply ACL"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Apply RLS"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Apply Tags"),(0,r.kt)("td",{parentName:"tr",align:null},"x"),(0,r.kt)("td",{parentName:"tr",align:null},"x")))),(0,r.kt)("h2",{id:"data-transformation"},"Data Transformation"),(0,r.kt)("p",null,"Simply write standard SQL et describe how you want the result to be stored in a YAML description file.\nThe major benefits Starlake bring to your Data transformation jobs are:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Write transformations in regular SQL or python scripts"),(0,r.kt)("li",{parentName:"ul"},"Use Jinja2 to augment your SQL scripts and make them easier to read and maintain"),(0,r.kt)("li",{parentName:"ul"},"Describe where and how the result is stored using YML description files (or even easier using an Excel sheet)"),(0,r.kt)("li",{parentName:"ul"},"Apply security to the target table"),(0,r.kt)("li",{parentName:"ul"},"Preview your data lifecycle and publish in SVG format")),(0,r.kt)("h2",{id:"illustration"},"Illustration"),(0,r.kt)("p",null,"The figure below illustrates how all the steps above are combined using the starlake CLI to provide a complete data lifecycle."),(0,r.kt)("p",null,(0,r.kt)("img",{alt:"Overview",src:a(81154).Z,width:"981",height:"2809"})))}m.isMDXComponent=!0},81154:(t,e,a)=>{a.d(e,{Z:()=>n});const n=a.p+"assets/images/data-all-steps-5fd5959b59261811c0f9e3c3b0873852.png"},8457:(t,e,a)=>{a.d(e,{Z:()=>n});const n=a.p+"assets/images/data-star-21b9f9730bff016e3a0dd5aee77b3cec.png"},67973:(t,e,a)=>{a.d(e,{Z:()=>n});const n=a.p+"assets/images/workflow-4c42599b0de7f282b5121082daf9df19.png"}}]);