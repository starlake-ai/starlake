"use strict";(self.webpackChunkstarlake_docs=self.webpackChunkstarlake_docs||[]).push([[9671],{3905:(e,t,a)=>{a.d(t,{Zo:()=>p,kt:()=>f});var i=a(7294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function n(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);t&&(i=i.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,i)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?n(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):n(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function o(e,t){if(null==e)return{};var a,i,r=function(e,t){if(null==e)return{};var a,i,r={},n=Object.keys(e);for(i=0;i<n.length;i++)a=n[i],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);for(i=0;i<n.length;i++)a=n[i],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=i.createContext({}),d=function(e){var t=i.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},p=function(e){var t=d(e.components);return i.createElement(s.Provider,{value:t},e.children)},u="mdxType",c={inlineCode:"code",wrapper:function(e){var t=e.children;return i.createElement(i.Fragment,{},t)}},m=i.forwardRef((function(e,t){var a=e.components,r=e.mdxType,n=e.originalType,s=e.parentName,p=o(e,["components","mdxType","originalType","parentName"]),u=d(a),m=r,f=u["".concat(s,".").concat(m)]||u[m]||c[m]||n;return a?i.createElement(f,l(l({ref:t},p),{},{components:a})):i.createElement(f,l({ref:t},p))}));function f(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var n=a.length,l=new Array(n);l[0]=m;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o[u]="string"==typeof e?e:r,l[1]=o;for(var d=2;d<n;d++)l[d]=a[d];return i.createElement.apply(null,l)}return i.createElement.apply(null,a)}m.displayName="MDXCreateElement"},9881:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>s,contentTitle:()=>l,default:()=>c,frontMatter:()=>n,metadata:()=>o,toc:()=>d});var i=a(7462),r=(a(7294),a(3905));const n={sidebar_position:1},l="What is Starlake ?",o={unversionedId:"intro",id:"intro",title:"What is Starlake ?",description:"Starlake is a configuration only Extract, Load and Transform engine.",source:"@site/docs/intro.md",sourceDirName:".",slug:"/intro",permalink:"/starlake/docs/next/intro",draft:!1,editUrl:"https://github.com/starlake-ai/starlake/edit/master/docs/docs/intro.md",tags:[],version:"current",sidebarPosition:1,frontMatter:{sidebar_position:1},sidebar:"starlakeSidebar",next:{title:"Concepts",permalink:"/starlake/docs/next/category/concepts"}},s={},d=[{value:"Data Extraction",id:"data-extraction",level:2},{value:"Data Loading",id:"data-loading",level:2},{value:"Data Transformation",id:"data-transformation",level:2},{value:"How it works",id:"how-it-works",level:2},{value:"BigQuery Data Pipeline",id:"bigquery-data-pipeline",level:3},{value:"Azure Databricks Data Pipeline",id:"azure-databricks-data-pipeline",level:3},{value:"On Premise Data Pipeline",id:"on-premise-data-pipeline",level:3},{value:"Google Cloud Storage Data Pipeline",id:"google-cloud-storage-data-pipeline",level:3}],p={toc:d},u="wrapper";function c(e){let{components:t,...n}=e;return(0,r.kt)(u,(0,i.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"what-is-starlake-"},"What is Starlake ?"),(0,r.kt)("p",null,"Starlake is a configuration only Extract, Load and Transform engine.\nThe workflow below is a typical use case :"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Extract your data as a set of Fixed Position, DSV (Delimiter-separated values) or JSON or XML files"),(0,r.kt)("li",{parentName:"ul"},"Define or infer the structure of each POSITION/DSV/JSON/XML file with a schema using YAML syntax"),(0,r.kt)("li",{parentName:"ul"},"Configure the loading process"),(0,r.kt)("li",{parentName:"ul"},"Start watching your data being available as Tables in your warehouse."),(0,r.kt)("li",{parentName:"ul"},"Build aggregates using SQL, Jinja and YAML configuration files.  ")),(0,r.kt)("p",null,"You may use Starlake for Extract, Load and Transform steps or any combination of these steps."),(0,r.kt)("h2",{id:"data-extraction"},"Data Extraction"),(0,r.kt)("p",null,"Starlake provides a fast way to extract, in full or incrementally, tables from your database."),(0,r.kt)("p",null,"Using parallel load through a JDBC connection and configuring the incremental fields in the schema, you may extract your data incrementally.\nOnce copied to the cloud provider of your choice, the data is available for further processing by the Load and Transform steps."),(0,r.kt)("h2",{id:"data-loading"},"Data Loading"),(0,r.kt)("p",null,"Usually, data loading is done by writing hand made custom parsers that transform input files into datasets of records."),(0,r.kt)("p",null,"Starlake aims at automating this parsing task by making data loading purely declarative."),(0,r.kt)("p",null,"The major benefits the Starlake data loader bring to your warehouse are:    "),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Eliminates manual coding for data loading"),(0,r.kt)("li",{parentName:"ul"},"Assign metadata to each dataset"),(0,r.kt)("li",{parentName:"ul"},"Expose data loading metrics and history"),(0,r.kt)("li",{parentName:"ul"},"Transform text files to strongly typed records without coding"),(0,r.kt)("li",{parentName:"ul"},"Support semantic types by allowing you to set type constraints on the incoming data"),(0,r.kt)("li",{parentName:"ul"},"Apply privacy to specific fields"),(0,r.kt)("li",{parentName:"ul"},"Apply security at load time"),(0,r.kt)("li",{parentName:"ul"},"Preview your data lifecycle and publish in SVG format"),(0,r.kt)("li",{parentName:"ul"},"Support multiple data sources and sinks"),(0,r.kt)("li",{parentName:"ul"},"Starlake is a very, very simple piece of software to administer")),(0,r.kt)("h2",{id:"data-transformation"},"Data Transformation"),(0,r.kt)("p",null,"Simply write standard SQL et describe how you want the result to be stored in a YAML description file.\nThe major benefits Starlake bring to your Data transformation jobs are:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Write transformations in regular SQL or python scripts"),(0,r.kt)("li",{parentName:"ul"},"Use Jinja2 to augment your SQL scripts and make them easier to read and maintain"),(0,r.kt)("li",{parentName:"ul"},"Describe where and how the result is stored using YML description files"),(0,r.kt)("li",{parentName:"ul"},"Apply security to the target table"),(0,r.kt)("li",{parentName:"ul"},"Preview your data lifecycle and publish in SVG format")),(0,r.kt)("h2",{id:"how-it-works"},"How it works"),(0,r.kt)("p",null,"Starlake Data Pipeline automates the loading and parsing of files and\ntheir ingestion into a warehouse where datasets become\navailable as strongly typed records."),(0,r.kt)("p",null,(0,r.kt)("img",{alt:"Complete Starlake Data Pipeline Workflow",src:a(2023).Z,title:"Complete Starlake Data Pipeline Workflow",width:"1946",height:"956"})),(0,r.kt)("p",null,"The figure above describes how Starlake implements the ",(0,r.kt)("inlineCode",{parentName:"p"},"Extract Load Transform (ELT)")," Data Pipeline steps.\nStarlake may be used indistinctly for all or any of these steps."),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"The ",(0,r.kt)("inlineCode",{parentName:"li"},"extract")," step allows to export selective data from an existing SQL database to a set of CSV files."),(0,r.kt)("li",{parentName:"ul"},"The ",(0,r.kt)("inlineCode",{parentName:"li"},"load")," step allows you to load text files, to ingest POSITION/CSV/JSON/XML files as strong typed records stored as parquet files or DWH tables (eq. Google BigQuery) or whatever sink you configured"),(0,r.kt)("li",{parentName:"ul"},"The ",(0,r.kt)("inlineCode",{parentName:"li"},"transform")," step allows to join loaded data and save them as parquet files, DWH tables or Elasticsearch indices")),(0,r.kt)("p",null,"The Load Transform steps support multiple configurations for inputs and outputs as illustrated in the\nfigure below. "),(0,r.kt)("p",null,(0,r.kt)("img",{alt:"Anywhere",src:a(5894).Z,title:"Anywhere",width:"1382",height:"814"})),(0,r.kt)("p",null,"Starlake Data Pipeline steps are described below:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Landing Area : In this optional step, files with predefined filename patterns are stored on a local filesystem in a predefined folder hierarchy"),(0,r.kt)("li",{parentName:"ul"},"Pending Area : Files associated with a schema are imported into this area."),(0,r.kt)("li",{parentName:"ul"},"Accepted Area : Pending files are parsed against their schema and records are rejected or accepted and made available in  Bigquery/Snowflake/Databricks/Hive/... tables or parquet files in a cloud bucket."),(0,r.kt)("li",{parentName:"ul"},"Business Area : Tables (Hive / BigQuery / Parquet files / ...) in the working area may be joined to provide a holistic view of the data through the definition of transformations."),(0,r.kt)("li",{parentName:"ul"},"Data visualization : parquet files / tables may be exposed in data warehouses or elasticsearch indices through an indexing definition")),(0,r.kt)("p",null,"Input file schemas, ingestion rules, transformation and indexing definitions used in the steps above are all defined in YAML files."),(0,r.kt)("h3",{id:"bigquery-data-pipeline"},"BigQuery Data Pipeline"),(0,r.kt)("p",null,(0,r.kt)("img",{alt:"Bigquery Workflow",src:a(8495).Z,width:"2290",height:"830"})),(0,r.kt)("h3",{id:"azure-databricks-data-pipeline"},"Azure Databricks Data Pipeline"),(0,r.kt)("p",null,(0,r.kt)("img",{alt:"Azure Workflow",src:a(3664).Z,width:"2290",height:"830"})),(0,r.kt)("h3",{id:"on-premise-data-pipeline"},"On Premise Data Pipeline"),(0,r.kt)("p",null,(0,r.kt)("img",{alt:"On Premise Workflow",src:a(7883).Z,width:"2290",height:"830"})),(0,r.kt)("h3",{id:"google-cloud-storage-data-pipeline"},"Google Cloud Storage Data Pipeline"),(0,r.kt)("p",null,(0,r.kt)("img",{alt:"Cloud Storage Workflow",src:a(3603).Z,width:"2290",height:"830"})))}c.isMDXComponent=!0},5894:(e,t,a)=>{a.d(t,{Z:()=>i});const i=a.p+"assets/images/anywhere-dfb894bb9ae7dd3d5b8738e7adf33349.png"},3664:(e,t,a)=>{a.d(t,{Z:()=>i});const i=a.p+"assets/images/elt-azure-databricks-d9a816edc2b87547a519f7b3d358c86c.png"},8495:(e,t,a)=>{a.d(t,{Z:()=>i});const i=a.p+"assets/images/elt-gcp-bq-26f9271ea7186b320a042f5710dcbb74.png"},3603:(e,t,a)=>{a.d(t,{Z:()=>i});const i=a.p+"assets/images/elt-gcp-gcs-8c3875d102d559d9f0239b4bb6fa5ac1.png"},7883:(e,t,a)=>{a.d(t,{Z:()=>i});const i=a.p+"assets/images/elt-onpremise-bb22b6b5e8a043d4eccb8d185e6034c5.png"},2023:(e,t,a)=>{a.d(t,{Z:()=>i});const i=a.p+"assets/images/workflow-16367736a7bea45138f2cd57a13130aa.png"}}]);