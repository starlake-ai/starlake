"use strict";(self.webpackChunkstarlake=self.webpackChunkstarlake||[]).push([[6313],{3905:(e,t,a)=>{a.d(t,{Zo:()=>p,kt:()=>u});var n=a(67294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function i(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?i(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):i(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function o(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},i=Object.keys(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),d=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},p=function(e){var t=d(e.components);return n.createElement(s.Provider,{value:t},e.children)},m="mdxType",g={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},h=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,i=e.originalType,s=e.parentName,p=o(e,["components","mdxType","originalType","parentName"]),m=d(a),h=r,u=m["".concat(s,".").concat(h)]||m[h]||g[h]||i;return a?n.createElement(u,l(l({ref:t},p),{},{components:a})):n.createElement(u,l({ref:t},p))}));function u(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=a.length,l=new Array(i);l[0]=h;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o[m]="string"==typeof e?e:r,l[1]=o;for(var d=2;d<i;d++)l[d]=a[d];return n.createElement.apply(null,l)}return n.createElement.apply(null,a)}h.displayName="MDXCreateElement"},7215:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>s,contentTitle:()=>l,default:()=>g,frontMatter:()=>i,metadata:()=>o,toc:()=>d});var n=a(87462),r=(a(67294),a(3905));const i={},l="Write strategies",o={unversionedId:"guides/load/write-strategies",id:"guides/load/write-strategies",title:"Write strategies",description:"When loading a file to a database table you can specify how to data is written to the table.",source:"@site/docs/0300-guides/200-load/160-write-strategies.mdx",sourceDirName:"0300-guides/200-load",slug:"/guides/load/write-strategies",permalink:"/starlake/docs/next/guides/load/write-strategies",draft:!1,editUrl:"https://github.com/starlake-ai/starlake/edit/master/docs/docs/0300-guides/200-load/160-write-strategies.mdx",tags:[],version:"current",sidebarPosition:160,frontMatter:{},sidebar:"starlakeSidebar",previous:{title:"Load strategies",permalink:"/starlake/docs/next/guides/load/load-strategies"},next:{title:"Clustering and Partitioning",permalink:"/starlake/docs/next/guides/load/sink"}},s={},d=[{value:"APPEND",id:"append",level:2},{value:"OVERWRITE",id:"overwrite",level:2},{value:"OVERWRITE_BY_KEY",id:"overwrite_by_key",level:2},{value:"OVERWRITE_BY_KEY_AND_TIMESTAMP",id:"overwrite_by_key_and_timestamp",level:2},{value:"OVERWRITE_BY_PARTITION",id:"overwrite_by_partition",level:2},{value:"DELETE_THEN_INSERT",id:"delete_then_insert",level:2},{value:"SCD2",id:"scd2",level:2},{value:"Adaptive write strategy",id:"adaptive-write-strategy",level:2},{value:"List of criterias",id:"list-of-criterias",level:3}],p={toc:d},m="wrapper";function g(e){let{components:t,...a}=e;return(0,r.kt)(m,(0,n.Z)({},p,a,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"write-strategies"},"Write strategies"),(0,r.kt)("p",null,"When loading a file to a database table you can specify how to data is written to the table.\nThat's what the ",(0,r.kt)("inlineCode",{parentName:"p"},"metadata.writeStrategy")," property is for."),(0,r.kt)("p",null,"The following strategies are available:"),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Strategy"),(0,r.kt)("th",{parentName:"tr",align:null},"Description"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"APPEND"),(0,r.kt)("td",{parentName:"tr",align:null},"Insert all rows into the table. If the table already contains data, the new rows will be appended.")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"OVERWRITE"),(0,r.kt)("td",{parentName:"tr",align:null},"Replace all rows in the table with the new rows. This will delete all existing rows and insert the new ones.")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"OVERWRITE_BY_KEY"),(0,r.kt)("td",{parentName:"tr",align:null},"Merge the new rows with the existing rows. If a row with the same key already exists in the table, the new row will overwrite the old one otherwise it will be appended.")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"OVERWRITE_BY_KEY_AND_TIMESTAMP"),(0,r.kt)("td",{parentName:"tr",align:null},"Merge the new rows with the existing rows. If a row with the same key and an older timestamp already exists in the table, the new row will overwrite the old one otherwise it will be appended.")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"OVERWRITE_BY_PARTITION"),(0,r.kt)("td",{parentName:"tr",align:null},"Merge the new rows with the existing rows. All existing partitions present in the new data will be overwritten. All other partitions will be left untouched and new partitions will be appended.")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"DELETE_THEN_INSERT"),(0,r.kt)("td",{parentName:"tr",align:null},"Delete rows in the target table for which records with the same keys exist in the incoming data, before inserting the incoming rows.")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"SCD2"),(0,r.kt)("td",{parentName:"tr",align:null},"Merge the new rows with the existing rows using the ",(0,r.kt)("a",{parentName:"td",href:"https://en.wikipedia.org/wiki/Slowly_changing_dimension"},"Slowly Changing Dimension Type 2")," strategy. This will keep track of the history of the rows in the table.")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"ADAPTATIVE"),(0,r.kt)("td",{parentName:"tr",align:null},"The write strategy will be determined at runtime based on the properties of the file being loaded.")))),(0,r.kt)("h2",{id:"append"},"APPEND"),(0,r.kt)("p",null,"If the table does not exist, it will be created. If the table already contains data, the new rows will be appended."),(0,r.kt)("br",null),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml",metastring:'title="Append to the table."',title:'"Append',to:!0,the:!0,'table."':!0},'table:\n  pattern: "<table>.*.csv"\n  metadata:\n    ...\n    writeStrategy:\n      type: "APPEND"\n  attributes:\n    - ...\n')),(0,r.kt)("h2",{id:"overwrite"},"OVERWRITE"),(0,r.kt)("p",null,"If the table does not exist, it will be created.\nReplace all rows in the table with the new rows. This will delete all existing rows and insert the new ones."),(0,r.kt)("br",null),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml",metastring:'title="Overwrite the table."',title:'"Overwrite',the:!0,'table."':!0},'table:\n  pattern: "<table>.*.csv"\n  metadata:\n    ...\n    writeStrategy:\n      type: "OVERWRITE"\n  attributes:\n    - ...\n')),(0,r.kt)("h2",{id:"overwrite_by_key"},"OVERWRITE_BY_KEY"),(0,r.kt)("p",null,"If the table does not exist, it will be created.\nMerge the new rows with the existing rows.\nIf a row with the same key already exists in the table,\nthe new row will overwrite the old one otherwise it will be appended."),(0,r.kt)("br",null),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml",metastring:"title=\"Overwrite by key. The column 'id' is used as key.\"",title:'"Overwrite',by:!0,"key.":!0,The:!0,column:!0,"'id'":!0,is:!0,used:!0,as:!0,'key."':!0},'table:\n  pattern: "<table>.*.csv"\n  metadata:\n    ...\n    writeStrategy:\n      type: "OVERWRITE_BY_KEY"\n      key: ["id"]\n      on: TARGET\n  attributes:\n    - ...\n')),(0,r.kt)("h2",{id:"overwrite_by_key_and_timestamp"},"OVERWRITE_BY_KEY_AND_TIMESTAMP"),(0,r.kt)("p",null,"If the table does not exist, it will be created.\nMerge the new rows with the existing rows. If a row with the same key and an older timestamp already exists in the table, the new row will overwrite the old one otherwise it will be appended."),(0,r.kt)("br",null),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml",metastring:"title=\"Overwrite by key and timestamp. The column 'id' is used as key.\"",title:'"Overwrite',by:!0,key:!0,and:!0,"timestamp.":!0,The:!0,column:!0,"'id'":!0,is:!0,used:!0,as:!0,'key."':!0},'table:\n  pattern: "<table>.*.csv"\n  metadata:\n    ...\n    writeStrategy:\n      type: "OVERWRITE_BY_KEY_AND_TIMESTAMP"\n      key: ["id"]\n      timestamp: "valid_from"\n      on: TARGET\n  attributes:\n    - ...\n')),(0,r.kt)("h2",{id:"overwrite_by_partition"},"OVERWRITE_BY_PARTITION"),(0,r.kt)("p",null,"If the table does not exist, it will be created.\nMerge the new rows with the existing rows. All existing partitions present in the new data will be overwritten. All other partitions will be left untouched and new partitions will be appended."),(0,r.kt)("br",null),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml",metastring:'title="Overwrite by partition. Require the partition property to be set in the metadata.sink section."',title:'"Overwrite',by:!0,"partition.":!0,Require:!0,the:!0,partition:!0,property:!0,to:!0,be:!0,set:!0,in:!0,"metadata.sink":!0,'section."':!0},'table:\n  pattern: "<table>.*.csv"\n  metadata:\n    ...\n    writeStrategy:\n      type: "OVERWRITE_BY_PARTITION"\n      on: TARGET\n  attributes:\n    - ...\n')),(0,r.kt)("h2",{id:"delete_then_insert"},"DELETE_THEN_INSERT"),(0,r.kt)("p",null,"If the table does not exist, it will be created.\nDelete rows in the target table for which records with the same keys exist in the incoming data, before inserting the incoming rows."),(0,r.kt)("br",null),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml",metastring:'title="Delete then insert"',title:'"Delete',then:!0,'insert"':!0},'table:\n  pattern: "<table>.*.csv"\n  metadata:\n    ...\n    writeStrategy:\n      type: "DELETE_THEN_INSERT"\n      key: ["id", "name" ...]\n  attributes:\n    - ...\n')),(0,r.kt)("h2",{id:"scd2"},"SCD2"),(0,r.kt)("p",null,"If the table does not exist, it will be created.\nMerge the new rows with the existing rows using the ",(0,r.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/Slowly_changing_dimension"},"Slowly Changing Dimension Type 2")," strategy. This will keep track of the history of the rows in the table."),(0,r.kt)("br",null),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml",metastring:"title=\"Slow changing dimension Type 2. The column 'id' is used as key and the column 'date' is used as timestamp.\"",title:'"Slow',changing:!0,dimension:!0,Type:!0,"2.":!0,The:!0,column:!0,"'id'":!0,is:!0,used:!0,as:!0,key:!0,and:!0,the:!0,"'date'":!0,'timestamp."':!0},'table:\n  pattern: "<table>.*.csv"\n  metadata:\n    ...\n    writeStrategy:\n      type: "SCD2"\n      key: ["id"]\n      timestamp: "date"\n      startTs: "valid_from"\n      endTs: "valid_to"\n      on: BOTH\n  attributes:\n    - ...\n')),(0,r.kt)("br",null),"the `startTs` and `endTs` properties are used to specify the names of the columns that will be used to store the start and end timestamps of the rows.",(0,r.kt)("p",null,"They may be omitted. In that case, the default values ",(0,r.kt)("inlineCode",{parentName:"p"},"sl_start_ts")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"sl_end_ts")," will be used."),(0,r.kt)("p",null,"These default values may be changed in the ",(0,r.kt)("inlineCode",{parentName:"p"},"metadata/application.sl.yml")," file by setting the following global variables ."),(0,r.kt)("br",null),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},'application:\n  ...\n  scd2StartTimestamp: "sl_start_ts"\n  scd2EndTimestamp: "sl_end_ts"\n')),(0,r.kt)("h2",{id:"adaptive-write-strategy"},"Adaptive write strategy"),(0,r.kt)("p",null,"Have you ever needed to change the way you feed your table from time to time or periodically?\nAdaptive Write may be the solution to your need. This feature allows you to adjust the loading mode at runtime,\naccording to various criteria listed in the table below."),(0,r.kt)("p",null,"For example, you want to ingest in APPEND mode throughout the week,\nexcept on Sundays when the source sends you all of certain tables,\nas discrepancies may occur with the incremental mode.\nThis can be done automatically by changing the domain or table configuration."),(0,r.kt)("p",null,"The example below illustrates the change at domain level that will be propagated to all these tables."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},"# _config.sl.yml\nload:\n   name: \"DOMAIN\n   metadata:\n      ...\n      writeStrategy:\n         types:\n             APPEND: 'dayOfWeek != 7'\n             OVERWRITE: 'dayOfWeek == 7'\n")),(0,r.kt)("p",null,"Another example is based on the file name:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},'# _config.sl.yml\nload:\n   name: "DOMAIN"\n   metadata:\n      ...\n      writeStrategy:\n         types:\n             OVERWRITE: \'group("mode") == "FULL"\'\n             APPEND: \'group("mode") == "APPEND"\'\n')),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},'#my_table.sl.yml\ntable:\n  ...\n  pattern: ".*-(?<mode>FULL|APPEND).csv$"\n')),(0,r.kt)("p",null,"You may want to combine these criteria. If so, just use regular boolean operators with ",(0,r.kt)("inlineCode",{parentName:"p"},"!"),", ",(0,r.kt)("inlineCode",{parentName:"p"},"&&"),"and ",(0,r.kt)("inlineCode",{parentName:"p"},"||")," and wrap with parenthesis if necessary."),(0,r.kt)("admonition",{type:"note"},(0,r.kt)("p",{parentName:"admonition"},"When using String in expression, makes sure to wrap them with double quotes ",(0,r.kt)("inlineCode",{parentName:"p"},'"'))),(0,r.kt)("h3",{id:"list-of-criterias"},"List of criterias"),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:"left"},"Criteria"),(0,r.kt)("th",{parentName:"tr",align:"left"},"Description"),(0,r.kt)("th",{parentName:"tr",align:"left"},"Example"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"left"},"group(index or name)"),(0,r.kt)("td",{parentName:"tr",align:"left"},"File pattern must use (named) capture groups"),(0,r.kt)("td",{parentName:"tr",align:"left"},"pattern: `my-file-(F")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"left"},"fileSize"),(0,r.kt)("td",{parentName:"tr",align:"left"},"Current file size in bytes"),(0,r.kt)("td",{parentName:"tr",align:"left"},"fileSize > 1000")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"left"},"fileSizeB"),(0,r.kt)("td",{parentName:"tr",align:"left"},"Current file size in bytes. Alias of fileSize"),(0,r.kt)("td",{parentName:"tr",align:"left"})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"left"},"fileSizeKo"),(0,r.kt)("td",{parentName:"tr",align:"left"},"Current file size in Ko"),(0,r.kt)("td",{parentName:"tr",align:"left"})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"left"},"fileSizeMo"),(0,r.kt)("td",{parentName:"tr",align:"left"},"Current file size in Mo"),(0,r.kt)("td",{parentName:"tr",align:"left"})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"left"},"fileSizeGo"),(0,r.kt)("td",{parentName:"tr",align:"left"},"Current file size in Go"),(0,r.kt)("td",{parentName:"tr",align:"left"})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"left"},"fileSizeTo"),(0,r.kt)("td",{parentName:"tr",align:"left"},"Current file size in To"),(0,r.kt)("td",{parentName:"tr",align:"left"})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"left"},"isFirstDayOfMonth"),(0,r.kt)("td",{parentName:"tr",align:"left"},"Current day is first day of month"),(0,r.kt)("td",{parentName:"tr",align:"left"})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"left"},"isLastDayOfMonth"),(0,r.kt)("td",{parentName:"tr",align:"left"},"Current day is last day of month"),(0,r.kt)("td",{parentName:"tr",align:"left"})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"left"},"dayOfWeek"),(0,r.kt)("td",{parentName:"tr",align:"left"},"Integer representing day of week. Monday = 1, ..., Sunday = 7"),(0,r.kt)("td",{parentName:"tr",align:"left"})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"left"},"isFileFirstDayOfMonth"),(0,r.kt)("td",{parentName:"tr",align:"left"},"File modification date is first day of month"),(0,r.kt)("td",{parentName:"tr",align:"left"})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"left"},"isFileLastDayOfMonth"),(0,r.kt)("td",{parentName:"tr",align:"left"},"File modification date is last day of month"),(0,r.kt)("td",{parentName:"tr",align:"left"})),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"left"},"fileDayOfWeek"),(0,r.kt)("td",{parentName:"tr",align:"left"},"Integer representing file modification day of week. Monday = 1, ..., Sunday = 7"),(0,r.kt)("td",{parentName:"tr",align:"left"})))),(0,r.kt)("admonition",{type:"note"},(0,r.kt)("p",{parentName:"admonition"},"For a criteria relying on datetime, you can change its timezone with ",(0,r.kt)("inlineCode",{parentName:"p"},"timezone")," application settings in ",(0,r.kt)("inlineCode",{parentName:"p"},"application.sl.yml"))))}g.isMDXComponent=!0}}]);