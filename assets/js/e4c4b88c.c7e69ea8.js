"use strict";(self.webpackChunkstarlake=self.webpackChunkstarlake||[]).push([[8614],{3905:(e,t,r)=>{r.d(t,{Zo:()=>c,kt:()=>m});var n=r(67294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var s=n.createContext({}),u=function(e){var t=n.useContext(s),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},c=function(e){var t=u(e.components);return n.createElement(s.Provider,{value:t},e.children)},p="mdxType",f={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},d=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,s=e.parentName,c=l(e,["components","mdxType","originalType","parentName"]),p=u(r),d=a,m=p["".concat(s,".").concat(d)]||p[d]||f[d]||o;return r?n.createElement(m,i(i({ref:t},c),{},{components:r})):n.createElement(m,i({ref:t},c))}));function m(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,i=new Array(o);i[0]=d;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[p]="string"==typeof e?e:a,i[1]=l;for(var u=2;u<o;u++)i[u]=r[u];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}d.displayName="MDXCreateElement"},86026:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>s,contentTitle:()=>i,default:()=>f,frontMatter:()=>o,metadata:()=>l,toc:()=>u});var n=r(87462),a=(r(67294),r(3905));const o={},i="Azure Synapse Spark Pools",l={unversionedId:"configuration/platforms/azure",id:"configuration/platforms/azure",title:"Azure Synapse Spark Pools",description:"Running Locally",source:"@site/docs/0500-configuration/0700-platforms/020.azure.md",sourceDirName:"0500-configuration/0700-platforms",slug:"/configuration/platforms/azure",permalink:"/starlake/docs/next/configuration/platforms/azure",draft:!1,editUrl:"https://github.com/starlake-ai/starlake/edit/master/docs/docs/0500-configuration/0700-platforms/020.azure.md",tags:[],version:"current",sidebarPosition:20,frontMatter:{},sidebar:"starlakeSidebar",previous:{title:"Amazon Web Services",permalink:"/starlake/docs/next/configuration/platforms/aws"},next:{title:"Databricks on any cloud",permalink:"/starlake/docs/next/configuration/platforms/databricks"}},s={},u=[{value:"Running Locally",id:"running-locally",level:2},{value:"Running on Azure",id:"running-on-azure",level:2}],c={toc:u},p="wrapper";function f(e){let{components:t,...r}=e;return(0,a.kt)(p,(0,n.Z)({},c,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"azure-synapse-spark-pools"},"Azure Synapse Spark Pools"),(0,a.kt)("h2",{id:"running-locally"},"Running Locally"),(0,a.kt)("p",null,"Starlake need to access ADFS. You need to provide the credentials in one of the three ways below:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Through a core-site.xml file present in the classpath (you'll probably use this method when running the ingestion process from your laptop):")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-xml"},' <?xml version="1.0" encoding="UTF-8"?>\n <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n <configuration>\n     <property>\n         <name>fs.azure.account.key.ebizcomet.dfs.core.windows.net</name>\n         <value>*******==</value>\n     </property>\n     <property>\n         <name>fs.default.name</name>\n         <value>abfs://cometfs@ebizcomet.dfs.core.windows.net/</value>\n     </property>\n </configuration>\n')),(0,a.kt)("h2",{id:"running-on-azure"},"Running on Azure"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"At cluster creation as specified ",(0,a.kt)("inlineCode",{parentName:"li"},"here <https://docs.microsoft.com/fr-fr/azure/databricks/data/data-sources/azure/azure-datalake-gen2#rdd-api>"),"_.\n(you'll probably use this method on a production cluster)")),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Through a specific application.conf file in the starlake-assembly.jar classpath.\nYou must add the spark.hadoop. prefix to the corresponding Hadoop configuration keys to propagate them to the Hadoop configurations that are used used in the Starlake Spark Job.")))}f.isMDXComponent=!0}}]);