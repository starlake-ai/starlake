"use strict";(self.webpackChunkstarlake=self.webpackChunkstarlake||[]).push([[5794],{3905:(t,e,a)=>{a.d(e,{Zo:()=>m,kt:()=>f});var n=a(67294);function r(t,e,a){return e in t?Object.defineProperty(t,e,{value:a,enumerable:!0,configurable:!0,writable:!0}):t[e]=a,t}function l(t,e){var a=Object.keys(t);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(t);e&&(n=n.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),a.push.apply(a,n)}return a}function i(t){for(var e=1;e<arguments.length;e++){var a=null!=arguments[e]?arguments[e]:{};e%2?l(Object(a),!0).forEach((function(e){r(t,e,a[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(a)):l(Object(a)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(a,e))}))}return t}function o(t,e){if(null==t)return{};var a,n,r=function(t,e){if(null==t)return{};var a,n,r={},l=Object.keys(t);for(n=0;n<l.length;n++)a=l[n],e.indexOf(a)>=0||(r[a]=t[a]);return r}(t,e);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(t);for(n=0;n<l.length;n++)a=l[n],e.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(t,a)&&(r[a]=t[a])}return r}var d=n.createContext({}),p=function(t){var e=n.useContext(d),a=e;return t&&(a="function"==typeof t?t(e):i(i({},e),t)),a},m=function(t){var e=p(t.components);return n.createElement(d.Provider,{value:e},t.children)},s="mdxType",u={inlineCode:"code",wrapper:function(t){var e=t.children;return n.createElement(n.Fragment,{},e)}},c=n.forwardRef((function(t,e){var a=t.components,r=t.mdxType,l=t.originalType,d=t.parentName,m=o(t,["components","mdxType","originalType","parentName"]),s=p(a),c=r,f=s["".concat(d,".").concat(c)]||s[c]||u[c]||l;return a?n.createElement(f,i(i({ref:e},m),{},{components:a})):n.createElement(f,i({ref:e},m))}));function f(t,e){var a=arguments,r=e&&e.mdxType;if("string"==typeof t||r){var l=a.length,i=new Array(l);i[0]=c;var o={};for(var d in e)hasOwnProperty.call(e,d)&&(o[d]=e[d]);o.originalType=t,o[s]="string"==typeof t?t:r,i[1]=o;for(var p=2;p<l;p++)i[p]=a[p];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}c.displayName="MDXCreateElement"},86369:(t,e,a)=>{a.r(e),a.d(e,{assets:()=>d,contentTitle:()=>i,default:()=>u,frontMatter:()=>l,metadata:()=>o,toc:()=>p});var n=a(87462),r=(a(67294),a(3905));const l={},i="Load DSV files",o={unversionedId:"guides/load/csv",id:"guides/load/csv",title:"Load DSV files",description:"File load configuration",source:"@site/docs/0300-guides/200-load/110-csv.mdx",sourceDirName:"0300-guides/200-load",slug:"/guides/load/csv",permalink:"/starlake/docs/next/guides/load/csv",draft:!1,editUrl:"https://github.com/starlake-ai/starlake/edit/master/docs/docs/0300-guides/200-load/110-csv.mdx",tags:[],version:"current",sidebarPosition:110,frontMatter:{},sidebar:"starlakeSidebar",previous:{title:"Load",permalink:"/starlake/docs/next/guides/load/load"},next:{title:"Load JSON Files",permalink:"/starlake/docs/next/guides/load/json"}},d={},p=[{value:"File load configuration",id:"file-load-configuration",level:2},{value:"Infer schema",id:"infer-schema",level:2},{value:"Parsing CSV",id:"parsing-csv",level:2},{value:"Attributes validation",id:"attributes-validation",level:2},{value:"Complete configuration",id:"complete-configuration",level:2}],m={toc:p},s="wrapper";function u(t){let{components:e,...a}=t;return(0,r.kt)(s,(0,n.Z)({},m,a,{components:e,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"load-dsv-files"},"Load DSV files"),(0,r.kt)("h2",{id:"file-load-configuration"},"File load configuration"),(0,r.kt)("p",null,"Most of the time, we won't need to define the table configuration, as the ",(0,r.kt)("inlineCode",{parentName:"p"},"infer-schema")," command will be able to infer the table configuration from the file itself using the `infer-schema command."),(0,r.kt)("p",null,"Sometimes, we still need to update some properties of the load configuration for the table or add new properties not present in the source file (ACL, comments  ...) or apply transformations."),(0,r.kt)("br",null),(0,r.kt)("p",null,"Describing the file you load involves defining :"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"the file pattern that is mapped to this schema,"),(0,r.kt)("li",{parentName:"ul"},"the parsing parameters and the materialization strategy (APPEND, OVERWRITE, OVERWRITE_BY_PARTITION, UPSERT_BY_KEY, UPSERT_BY_KEY_AND_TIMESTAMP, SCD2 ...)"),(0,r.kt)("li",{parentName:"ul"},"the file format as a list of attributes.")),(0,r.kt)("h2",{id:"infer-schema"},"Infer schema"),(0,r.kt)("p",null,"The very first step is to infer the schema of the file from a data file as described in the ",(0,r.kt)("a",{parentName:"p",href:"autoload#how-autoload-detects-the-format-of-the-files"},"autoload section")," before you start customizing your configuration. This is done using the ",(0,r.kt)("inlineCode",{parentName:"p"},"infer-schema")," command. This will bootstrap the configuration file for the table."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-bash"},"\nstarlake infer-schema --input-path incoming/starbake\n\n")),(0,r.kt)("h2",{id:"parsing-csv"},"Parsing CSV"),(0,r.kt)("p",null,"CSV Parsing options are defined in the ",(0,r.kt)("inlineCode",{parentName:"p"},"metadata")," section of the configuration file."),(0,r.kt)("br",null),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml",metastring:'title="metadata/load/<domain>/<table>.sl.yml - parsing section"',title:'"metadata/load/<domain>/<table>.sl.yml',"-":!0,parsing:!0,'section"':!0},'table:\n  pattern: "order_line.*.csv"\n  metadata:\n    format: "DSV"\n    withHeader: true\n    separator: ";"\n    ...\n  attributes:\n    - ...\n')),(0,r.kt)("hr",null),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Attribute"),(0,r.kt)("th",{parentName:"tr",align:null},"Description"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"encoding"),(0,r.kt)("td",{parentName:"tr",align:null},"the encoding of the file. Default is ",(0,r.kt)("inlineCode",{parentName:"td"},"UTF-8"))),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"withHeader"),(0,r.kt)("td",{parentName:"tr",align:null},"if the file contains a header. Default is ",(0,r.kt)("inlineCode",{parentName:"td"},"true"))),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"separator"),(0,r.kt)("td",{parentName:"tr",align:null},"the separator used in the file. Default is ",(0,r.kt)("inlineCode",{parentName:"td"},";"))),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"quote"),(0,r.kt)("td",{parentName:"tr",align:null},"the quote character used in the file. Default is ",(0,r.kt)("inlineCode",{parentName:"td"},'"'))),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"escape"),(0,r.kt)("td",{parentName:"tr",align:null},"the escape character used in the file. Default is ",(0,r.kt)("inlineCode",{parentName:"td"},"\\"))),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"ignore"),(0,r.kt)("td",{parentName:"tr",align:null},"a regex to filter lines to ignore. All lines matching the regex will be ignored. Default is None")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"ack"),(0,r.kt)("td",{parentName:"tr",align:null},"load file only if a file with the same name and with this extension exist in the same directory.")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"options"),(0,r.kt)("td",{parentName:"tr",align:null},"a map of options to pass to the parser. Available options are defined in the ",(0,r.kt)("a",{parentName:"td",href:"https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option"},"Apache Spark CSV documentation"))),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"emptyIsNull"),(0,r.kt)("td",{parentName:"tr",align:null},"if empty column values should be considered as null. Default is ",(0,r.kt)("inlineCode",{parentName:"td"},"false"))),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"fillWithDefaultValue"),(0,r.kt)("td",{parentName:"tr",align:null},"if the null value should be filled with the default value of the column. Default is ",(0,r.kt)("inlineCode",{parentName:"td"},"false"))))),(0,r.kt)("h2",{id:"attributes-validation"},"Attributes validation"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml",metastring:'title="metadata/load/<domain>/<table>.sl.yml - validation section"',title:'"metadata/load/<domain>/<table>.sl.yml',"-":!0,validation:!0,'section"':!0},'table:\n  pattern: "order_line.*.csv"\n  metadata:\n    - ...\n  attributes:\n  - name: "order_id"\n    type: "int"\n  - name: "product_id"\n    type: "int"\n  - name: "quantity"\n    type: "int"\n  - name: "sale_price"\n    type: "double"\n')),(0,r.kt)("br",null),"The attributes properties are defined in the `attributes` section of the configuration file.",(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Attribute"),(0,r.kt)("th",{parentName:"tr",align:null},"Description"),(0,r.kt)("th",{parentName:"tr",align:null},"Default"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"name"),(0,r.kt)("td",{parentName:"tr",align:null},"the name of the column in the file header. If no header is present, this refers to the column index in the file. This is the only required property. this will also refer to the column name in the table except if the ",(0,r.kt)("inlineCode",{parentName:"td"},"rename")," property is set."),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"type"),(0,r.kt)("td",{parentName:"tr",align:null},"the type of the column. Default is ",(0,r.kt)("inlineCode",{parentName:"td"},"string"),". Available types are ",(0,r.kt)("inlineCode",{parentName:"td"},"string"),", ",(0,r.kt)("inlineCode",{parentName:"td"},"int"),", ",(0,r.kt)("inlineCode",{parentName:"td"},"long"),", ",(0,r.kt)("inlineCode",{parentName:"td"},"double"),", ",(0,r.kt)("inlineCode",{parentName:"td"},"float"),", ",(0,r.kt)("inlineCode",{parentName:"td"},"boolean"),", any flavor of ",(0,r.kt)("inlineCode",{parentName:"td"},"date")," or ",(0,r.kt)("inlineCode",{parentName:"td"},"timestamp")," as defined in the ",(0,r.kt)("inlineCode",{parentName:"td"},"metadata/types/default.sl.yml")," file. Also note that you can add your own types in the ",(0,r.kt)("inlineCode",{parentName:"td"},"metadata/types")," directory."),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"required"),(0,r.kt)("td",{parentName:"tr",align:null},"if the column is required to have a non null value (see ",(0,r.kt)("inlineCode",{parentName:"td"},"nullValue")," in the ",(0,r.kt)("inlineCode",{parentName:"td"},"metadata.options")," section)."),(0,r.kt)("td",{parentName:"tr",align:null},"false")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"privacy"),(0,r.kt)("td",{parentName:"tr",align:null},"a transformation to apply to the column."),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"transform"),(0,r.kt)("td",{parentName:"tr",align:null},"a transformation to apply to the column."),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"comment"),(0,r.kt)("td",{parentName:"tr",align:null},"a comment for the column."),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"rename"),(0,r.kt)("td",{parentName:"tr",align:null},"the name of the column in the database."),(0,r.kt)("td",{parentName:"tr",align:null},(0,r.kt)("inlineCode",{parentName:"td"},"name"))),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"default"),(0,r.kt)("td",{parentName:"tr",align:null},"the default value of the column."),(0,r.kt)("td",{parentName:"tr",align:null})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"trim"),(0,r.kt)("td",{parentName:"tr",align:null},"the trim strategy for the column.  Available trim strategies are ",(0,r.kt)("inlineCode",{parentName:"td"},"None"),", ",(0,r.kt)("inlineCode",{parentName:"td"},"Left"),", ",(0,r.kt)("inlineCode",{parentName:"td"},"Right"),", ",(0,r.kt)("inlineCode",{parentName:"td"},"Both")),(0,r.kt)("td",{parentName:"tr",align:null})))),(0,r.kt)("p",null,"To add / replace or ignore some attributes, check the ",(0,r.kt)("a",{parentName:"p",href:"transform"},"Transform on load")," section."),(0,r.kt)("h2",{id:"complete-configuration"},"Complete configuration"),(0,r.kt)("p",null,"The name property of the column is the only required field. The type property is optional and will be set to ",(0,r.kt)("inlineCode",{parentName:"p"},"string")," if not provided.\nThe configuration file below describes how the ",(0,r.kt)("inlineCode",{parentName:"p"},"order_line")," files should be loaded."),(0,r.kt)("br",null),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml",metastring:'title="metadata/load/<domain>/<table>.sl.yml"',title:'"metadata/load/<domain>/<table>.sl.yml"'},'table:\n  pattern: "order_line.*.csv"\n  metadata:\n    format: "DSV"\n    withHeader: true\n    separator: ";"\n    writeStrategy:\n      type: "APPEND"\n  attributes:\n  - name: "order_id"\n    type: "int"\n  - name: "product_id"\n    type: "int"\n  - name: "quantity"\n    type: "int"\n  - name: "sale_price"\n    type: "double"\n')))}u.isMDXComponent=!0}}]);