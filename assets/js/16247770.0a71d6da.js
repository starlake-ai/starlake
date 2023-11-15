"use strict";(self.webpackChunkstarlake_docs=self.webpackChunkstarlake_docs||[]).push([[6877],{3905:(t,e,a)=>{a.d(e,{Zo:()=>u,kt:()=>k});var r=a(7294);function n(t,e,a){return e in t?Object.defineProperty(t,e,{value:a,enumerable:!0,configurable:!0,writable:!0}):t[e]=a,t}function l(t,e){var a=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),a.push.apply(a,r)}return a}function i(t){for(var e=1;e<arguments.length;e++){var a=null!=arguments[e]?arguments[e]:{};e%2?l(Object(a),!0).forEach((function(e){n(t,e,a[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(a)):l(Object(a)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(a,e))}))}return t}function o(t,e){if(null==t)return{};var a,r,n=function(t,e){if(null==t)return{};var a,r,n={},l=Object.keys(t);for(r=0;r<l.length;r++)a=l[r],e.indexOf(a)>=0||(n[a]=t[a]);return n}(t,e);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(t);for(r=0;r<l.length;r++)a=l[r],e.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(t,a)&&(n[a]=t[a])}return n}var p=r.createContext({}),d=function(t){var e=r.useContext(p),a=e;return t&&(a="function"==typeof t?t(e):i(i({},e),t)),a},u=function(t){var e=d(t.components);return r.createElement(p.Provider,{value:e},t.children)},m="mdxType",s={inlineCode:"code",wrapper:function(t){var e=t.children;return r.createElement(r.Fragment,{},e)}},c=r.forwardRef((function(t,e){var a=t.components,n=t.mdxType,l=t.originalType,p=t.parentName,u=o(t,["components","mdxType","originalType","parentName"]),m=d(a),c=n,k=m["".concat(p,".").concat(c)]||m[c]||s[c]||l;return a?r.createElement(k,i(i({ref:e},u),{},{components:a})):r.createElement(k,i({ref:e},u))}));function k(t,e){var a=arguments,n=e&&e.mdxType;if("string"==typeof t||n){var l=a.length,i=new Array(l);i[0]=c;var o={};for(var p in e)hasOwnProperty.call(e,p)&&(o[p]=e[p]);o.originalType=t,o[m]="string"==typeof t?t:n,i[1]=o;for(var d=2;d<l;d++)i[d]=a[d];return r.createElement.apply(null,i)}return r.createElement.apply(null,a)}c.displayName="MDXCreateElement"},642:(t,e,a)=>{a.r(e),a.d(e,{assets:()=>p,contentTitle:()=>i,default:()=>s,frontMatter:()=>l,metadata:()=>o,toc:()=>d});var r=a(7462),n=(a(7294),a(3905));const l={sidebar_position:40,title:"bqload"},i=void 0,o={unversionedId:"cli/bqload",id:"version-1.0.0/cli/bqload",title:"bqload",description:"Synopsis",source:"@site/versioned_docs/version-1.0.0/cli/bqload.md",sourceDirName:"cli",slug:"/cli/bqload",permalink:"/starlake/docs/cli/bqload",draft:!1,editUrl:"https://github.com/starlake-ai/starlake/edit/master/docs/versioned_docs/version-1.0.0/cli/bqload.md",tags:[],version:"1.0.0",sidebarPosition:40,frontMatter:{sidebar_position:40,title:"bqload"},sidebar:"starlakeSidebar",previous:{title:"bq2yml or bq-info",permalink:"/starlake/docs/cli/bq-info"},next:{title:"cnxload",permalink:"/starlake/docs/cli/cnxload"}},p={},d=[{value:"Synopsis",id:"synopsis",level:2},{value:"Description",id:"description",level:2},{value:"Parameters",id:"parameters",level:2}],u={toc:d},m="wrapper";function s(t){let{components:e,...a}=t;return(0,n.kt)(m,(0,r.Z)({},u,a,{components:e,mdxType:"MDXLayout"}),(0,n.kt)("h2",{id:"synopsis"},"Synopsis"),(0,n.kt)("p",null,(0,n.kt)("strong",{parentName:"p"},"starlake bqload ","[options]")),(0,n.kt)("h2",{id:"description"},"Description"),(0,n.kt)("h2",{id:"parameters"},"Parameters"),(0,n.kt)("table",null,(0,n.kt)("thead",{parentName:"table"},(0,n.kt)("tr",{parentName:"thead"},(0,n.kt)("th",{parentName:"tr",align:null},"Parameter"),(0,n.kt)("th",{parentName:"tr",align:null},"Cardinality"),(0,n.kt)("th",{parentName:"tr",align:null},"Description"))),(0,n.kt)("tbody",{parentName:"table"},(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},"--source_file:",(0,n.kt)("inlineCode",{parentName:"td"},"<value>")),(0,n.kt)("td",{parentName:"tr",align:null},(0,n.kt)("em",{parentName:"td"},"Required")),(0,n.kt)("td",{parentName:"tr",align:null},"Full Path to source file")),(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},"--output_database:",(0,n.kt)("inlineCode",{parentName:"td"},"<value>")),(0,n.kt)("td",{parentName:"tr",align:null},(0,n.kt)("em",{parentName:"td"},"Optional")),(0,n.kt)("td",{parentName:"tr",align:null},"Target BigQuery Project")),(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},"--output_dataset:",(0,n.kt)("inlineCode",{parentName:"td"},"<value>")),(0,n.kt)("td",{parentName:"tr",align:null},(0,n.kt)("em",{parentName:"td"},"Required")),(0,n.kt)("td",{parentName:"tr",align:null},"BigQuery Output Dataset")),(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},"--output_table:",(0,n.kt)("inlineCode",{parentName:"td"},"<value>")),(0,n.kt)("td",{parentName:"tr",align:null},(0,n.kt)("em",{parentName:"td"},"Required")),(0,n.kt)("td",{parentName:"tr",align:null},"BigQuery Output Table")),(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},"--output_partition:",(0,n.kt)("inlineCode",{parentName:"td"},"<value>")),(0,n.kt)("td",{parentName:"tr",align:null},(0,n.kt)("em",{parentName:"td"},"Optional")),(0,n.kt)("td",{parentName:"tr",align:null},"BigQuery Partition Field")),(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},"--require_partition_filter:",(0,n.kt)("inlineCode",{parentName:"td"},"<value>")),(0,n.kt)("td",{parentName:"tr",align:null},(0,n.kt)("em",{parentName:"td"},"Optional")),(0,n.kt)("td",{parentName:"tr",align:null},"Require Partition Filter")),(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},"--output_clustering:",(0,n.kt)("inlineCode",{parentName:"td"},"col1,col2...")),(0,n.kt)("td",{parentName:"tr",align:null},(0,n.kt)("em",{parentName:"td"},"Optional")),(0,n.kt)("td",{parentName:"tr",align:null},"BigQuery Clustering Fields")),(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},"--connectionRef:",(0,n.kt)("inlineCode",{parentName:"td"},"<value>")),(0,n.kt)("td",{parentName:"tr",align:null},(0,n.kt)("em",{parentName:"td"},"Optional")),(0,n.kt)("td",{parentName:"tr",align:null},"BigQuery Connector")),(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},"--source_format:",(0,n.kt)("inlineCode",{parentName:"td"},"<value>")),(0,n.kt)("td",{parentName:"tr",align:null},(0,n.kt)("em",{parentName:"td"},"Optional")),(0,n.kt)("td",{parentName:"tr",align:null},"Source Format eq. parquet. This option is ignored, Only parquet source format is supported at this time")),(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},"--create_disposition:",(0,n.kt)("inlineCode",{parentName:"td"},"<value>")),(0,n.kt)("td",{parentName:"tr",align:null},(0,n.kt)("em",{parentName:"td"},"Optional")),(0,n.kt)("td",{parentName:"tr",align:null},"Big Query Create disposition ",(0,n.kt)("a",{parentName:"td",href:"https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/CreateDisposition"},"https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/CreateDisposition"))),(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},"--write_disposition:",(0,n.kt)("inlineCode",{parentName:"td"},"<value>")),(0,n.kt)("td",{parentName:"tr",align:null},(0,n.kt)("em",{parentName:"td"},"Optional")),(0,n.kt)("td",{parentName:"tr",align:null},"Big Query Write disposition ",(0,n.kt)("a",{parentName:"td",href:"https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/WriteDisposition"},"https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/WriteDisposition"))),(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},"--row_level_security:",(0,n.kt)("inlineCode",{parentName:"td"},"<value>")),(0,n.kt)("td",{parentName:"tr",align:null},(0,n.kt)("em",{parentName:"td"},"Optional")),(0,n.kt)("td",{parentName:"tr",align:null},"value is in the form name,filter,sa:",(0,n.kt)("a",{parentName:"td",href:"mailto:sa@mail.com"},"sa@mail.com"),",user:",(0,n.kt)("a",{parentName:"td",href:"mailto:user@mail.com"},"user@mail.com"),",group:",(0,n.kt)("a",{parentName:"td",href:"mailto:group@mail.com"},"group@mail.com"))))))}s.isMDXComponent=!0}}]);