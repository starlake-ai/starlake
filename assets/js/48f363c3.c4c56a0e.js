"use strict";(self.webpackChunkstarlake=self.webpackChunkstarlake||[]).push([[6065],{3905:(e,t,a)=>{a.d(t,{Zo:()=>c,kt:()=>f});var n=a(67294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function i(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},o=Object.keys(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),u=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},c=function(e){var t=u(e.components);return n.createElement(s.Provider,{value:t},e.children)},p="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,o=e.originalType,s=e.parentName,c=i(e,["components","mdxType","originalType","parentName"]),p=u(a),m=r,f=p["".concat(s,".").concat(m)]||p[m]||d[m]||o;return a?n.createElement(f,l(l({ref:t},c),{},{components:a})):n.createElement(f,l({ref:t},c))}));function f(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=a.length,l=new Array(o);l[0]=m;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i[p]="string"==typeof e?e:r,l[1]=i;for(var u=2;u<o;u++)l[u]=a[u];return n.createElement.apply(null,l)}return n.createElement.apply(null,a)}m.displayName="MDXCreateElement"},85162:(e,t,a)=>{a.d(t,{Z:()=>l});var n=a(67294),r=a(86010);const o={tabItem:"tabItem_Ymn6"};function l(e){let{children:t,hidden:a,className:l}=e;return n.createElement("div",{role:"tabpanel",className:(0,r.Z)(o.tabItem,l),hidden:a},t)}},74866:(e,t,a)=>{a.d(t,{Z:()=>w});var n=a(87462),r=a(67294),o=a(86010),l=a(12466),i=a(16550),s=a(91980),u=a(67392),c=a(50012);function p(e){return function(e){return r.Children.map(e,(e=>{if(!e||(0,r.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)}))?.filter(Boolean)??[]}(e).map((e=>{let{props:{value:t,label:a,attributes:n,default:r}}=e;return{value:t,label:a,attributes:n,default:r}}))}function d(e){const{values:t,children:a}=e;return(0,r.useMemo)((()=>{const e=t??p(a);return function(e){const t=(0,u.l)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error(`Docusaurus error: Duplicate values "${t.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`)}(e),e}),[t,a])}function m(e){let{value:t,tabValues:a}=e;return a.some((e=>e.value===t))}function f(e){let{queryString:t=!1,groupId:a}=e;const n=(0,i.k6)(),o=function(e){let{queryString:t=!1,groupId:a}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!a)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return a??null}({queryString:t,groupId:a});return[(0,s._X)(o),(0,r.useCallback)((e=>{if(!o)return;const t=new URLSearchParams(n.location.search);t.set(o,e),n.replace({...n.location,search:t.toString()})}),[o,n])]}function h(e){const{defaultValue:t,queryString:a=!1,groupId:n}=e,o=d(e),[l,i]=(0,r.useState)((()=>function(e){let{defaultValue:t,tabValues:a}=e;if(0===a.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!m({value:t,tabValues:a}))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${t}" but none of its children has the corresponding value. Available values are: ${a.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);return t}const n=a.find((e=>e.default))??a[0];if(!n)throw new Error("Unexpected error: 0 tabValues");return n.value}({defaultValue:t,tabValues:o}))),[s,u]=f({queryString:a,groupId:n}),[p,h]=function(e){let{groupId:t}=e;const a=function(e){return e?`docusaurus.tab.${e}`:null}(t),[n,o]=(0,c.Nk)(a);return[n,(0,r.useCallback)((e=>{a&&o.set(e)}),[a,o])]}({groupId:n}),b=(()=>{const e=s??p;return m({value:e,tabValues:o})?e:null})();(0,r.useLayoutEffect)((()=>{b&&i(b)}),[b]);return{selectedValue:l,selectValue:(0,r.useCallback)((e=>{if(!m({value:e,tabValues:o}))throw new Error(`Can't select invalid tab value=${e}`);i(e),u(e),h(e)}),[u,h,o]),tabValues:o}}var b=a(72389);const k={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"};function g(e){let{className:t,block:a,selectedValue:i,selectValue:s,tabValues:u}=e;const c=[],{blockElementScrollPositionUntilNextRender:p}=(0,l.o5)(),d=e=>{const t=e.currentTarget,a=c.indexOf(t),n=u[a].value;n!==i&&(p(t),s(n))},m=e=>{let t=null;switch(e.key){case"Enter":d(e);break;case"ArrowRight":{const a=c.indexOf(e.currentTarget)+1;t=c[a]??c[0];break}case"ArrowLeft":{const a=c.indexOf(e.currentTarget)-1;t=c[a]??c[c.length-1];break}}t?.focus()};return r.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,o.Z)("tabs",{"tabs--block":a},t)},u.map((e=>{let{value:t,label:a,attributes:l}=e;return r.createElement("li",(0,n.Z)({role:"tab",tabIndex:i===t?0:-1,"aria-selected":i===t,key:t,ref:e=>c.push(e),onKeyDown:m,onClick:d},l,{className:(0,o.Z)("tabs__item",k.tabItem,l?.className,{"tabs__item--active":i===t})}),a??t)})))}function y(e){let{lazy:t,children:a,selectedValue:n}=e;const o=(Array.isArray(a)?a:[a]).filter(Boolean);if(t){const e=o.find((e=>e.props.value===n));return e?(0,r.cloneElement)(e,{className:"margin-top--md"}):null}return r.createElement("div",{className:"margin-top--md"},o.map(((e,t)=>(0,r.cloneElement)(e,{key:t,hidden:e.props.value!==n}))))}function v(e){const t=h(e);return r.createElement("div",{className:(0,o.Z)("tabs-container",k.tabList)},r.createElement(g,(0,n.Z)({},e,t)),r.createElement(y,(0,n.Z)({},e,t)))}function w(e){const t=(0,b.Z)();return r.createElement(v,(0,n.Z)({key:String(t)},e))}},16309:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>c,contentTitle:()=>s,default:()=>f,frontMatter:()=>i,metadata:()=>u,toc:()=>p});var n=a(87462),r=(a(67294),a(3905)),o=a(74866),l=a(85162);const i={},s="Bootstrap project",u={unversionedId:"user-guide/bootstrap",id:"version-1.1.0/user-guide/bootstrap",title:"Bootstrap project",description:"Create sample project",source:"@site/versioned_docs/version-1.1.0/0400-user-guide/050.bootstrap.mdx",sourceDirName:"0400-user-guide",slug:"/user-guide/bootstrap",permalink:"/starlake/docs/user-guide/bootstrap",draft:!1,editUrl:"https://github.com/starlake-ai/starlake/edit/master/docs/versioned_docs/version-1.1.0/0400-user-guide/050.bootstrap.mdx",tags:[],version:"1.1.0",sidebarPosition:50,frontMatter:{},sidebar:"starlakeSidebar",previous:{title:"User guide",permalink:"/starlake/docs/category/user-guide"},next:{title:"Infer schemas",permalink:"/starlake/docs/user-guide/infer-schema"}},c={},p=[{value:"Create sample project",id:"create-sample-project",level:2},{value:"Understanding the project hierarchy",id:"understanding-the-project-hierarchy",level:2},{value:"Project configuration",id:"project-configuration",level:2},{value:"Next steps",id:"next-steps",level:2}],d={toc:p},m="wrapper";function f(e){let{components:t,...a}=e;return(0,r.kt)(m,(0,n.Z)({},d,a,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"bootstrap-project"},"Bootstrap project"),(0,r.kt)("h2",{id:"create-sample-project"},"Create sample project"),(0,r.kt)("p",null,"To create a new project, you need first to create an empty folder and run the\nstarlake bootstrap CLI command from there:"),(0,r.kt)(o.Z,{groupId:"platforms",mdxType:"Tabs"},(0,r.kt)(l.Z,{value:"linux_macos",label:"Linux/MacOS",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"$ mkdir $HOME/userguide\n$ cd $HOME/userguide\n$ starlake bootstrap\n"))),(0,r.kt)(l.Z,{value:"windows",label:"Windows",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"c:\\> mkdir c:\\userguide\nc:\\> cd c:\\userguide\nc:\\> starlake bootstrap\n"))),(0,r.kt)(l.Z,{value:"docker",label:"Docker",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"$ mkdir $HOME/userguide\n$ cd $HOME/userguide\n$ docker run  -v `pwd`:/app/userguide -e SL_ROOT=/app/userguide -it starlake bootstrap\n")))),(0,r.kt)("admonition",{type:"note"},(0,r.kt)("p",{parentName:"admonition"},"By default, the project will be created in the current working directory.\nTo bootstrap the project in a different folder set SL_ROOT env variable to that folder:")),(0,r.kt)(o.Z,{groupId:"platforms",mdxType:"Tabs"},(0,r.kt)(l.Z,{value:"linux_macos",label:"Linux/MacOS",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"$ SL_ROOT=/my/other/location starlake bootstrap\n"))),(0,r.kt)(l.Z,{value:"windows",label:"Windows",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"c:\\> set SL_ROOT=c:\\my\\other\\location\nc:\\> starlake bootstrap\n"))),(0,r.kt)(l.Z,{value:"docker",label:"Docker",mdxType:"TabItem"},"You are always required to set the SL_ROOT env var when using Docker",(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"$ mkdir $HOME/userguide\n$ cd $HOME/userguide\n$ docker run  -v `pwd`:/app/userguide -e SL_ROOT=/app/userguide -it starlake bootstrap\n")))),(0,r.kt)("p",null,"The ",(0,r.kt)("inlineCode",{parentName:"p"},"bootstrap")," command will ask you to select a project template from the list below:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-text",metastring:"{3}","{3}":!0},"Please choose a template:\n  0. initializer\n  1. userguide\n  2. starbake\n  q. quit\n=> 1\n")),(0,r.kt)("p",null,"Please choose ",(0,r.kt)("inlineCode",{parentName:"p"},"1. userguide")," to create a project that comes with sample datasets and be able to follow this tutorial.\nstarlake will then create a default project hierarchy that allow you to start extracting, loading and transforming data."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-text",metastring:"{2,5,7,8,9,10}","{2,5,7,8,9,10}":!0},".\n\u251c\u2500\u2500 metadata\n\u2502   \u251c\u2500\u2500 application.sl.yml\n\u2502   \u251c\u2500\u2500 env.sl.yml\n\u2502   \u251c\u2500\u2500 expectations\n\u2502   \u2502   \u2514\u2500\u2500 expectations.sl.yml\n\u2502   \u251c\u2500\u2500 extract\n\u2502   \u251c\u2500\u2500 load\n\u2502   \u251c\u2500\u2500 transform\n\u2502   \u251c\u2500\u2500 types\n\u2502   \u2502   \u251c\u2500\u2500 default.sl.yml\n\u2514\u2500\u2500 sample-data\n    \u251c\u2500\u2500 hr\n    \u2502   \u251c\u2500\u2500 locations-2018-01-01.json\n    \u2502   \u2514\u2500\u2500 sellers-2018-01-01.json\n    \u2514\u2500\u2500 sales\n        \u251c\u2500\u2500 customers-2018-01-01.psv\n        \u2514\u2500\u2500 orders-2018-01-01.csv\n\n")),(0,r.kt)("h2",{id:"understanding-the-project-hierarchy"},"Understanding the project hierarchy"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"The ",(0,r.kt)("inlineCode",{parentName:"li"},"incoming")," folder host all the files you will want to load into your warehouse. This is explained in more detail in the ",(0,r.kt)("a",{parentName:"li",href:"load"},"Move data section")),(0,r.kt)("li",{parentName:"ul"},"The ",(0,r.kt)("inlineCode",{parentName:"li"},"metadata")," folder contains the ",(0,r.kt)("strong",{parentName:"li"},"extract"),", ",(0,r.kt)("strong",{parentName:"li"},"load")," and ",(0,r.kt)("strong",{parentName:"li"},"transform")," configuration files"),(0,r.kt)("li",{parentName:"ul"},"The ",(0,r.kt)("inlineCode",{parentName:"li"},"sample-data")," folder contains sample data that you can use to test your project"),(0,r.kt)("li",{parentName:"ul"},"The ",(0,r.kt)("inlineCode",{parentName:"li"},"expectations")," folder contains the expectations configuration files used for validating the data loaded / transformed in your warehouse")),(0,r.kt)("h2",{id:"project-configuration"},"Project configuration"),(0,r.kt)("p",null,"The project configuration is stored in the ",(0,r.kt)("inlineCode",{parentName:"p"},"metadata/application.sl.yml")," file. This file contains the project version and the list of connections to the different data sinks."),(0,r.kt)("p",null,"This application configuration file contains multiple connections:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Each connection is a sink where data can be loaded/transformed"),(0,r.kt)("li",{parentName:"ul"},"The active connection to use for loading/transforming data is specified in the connectionRef property of the application section"),(0,r.kt)("li",{parentName:"ul"},"The connectionRef property can be set to any of the connection names defined in the connections section below."),(0,r.kt)("li",{parentName:"ul"},"The example below set the active connectionRef using the ",(0,r.kt)("inlineCode",{parentName:"li"},"connectionRef"),"variable")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},'\napplication:\n  version: "1.0.0"\n  # the connection to use for loading/transforming data. See connections section below\n  connectionRef: "sparkLocal"\n  connections:\n    sparkLocal:\n      type: "fs"\n\n')),(0,r.kt)("h2",{id:"next-steps"},"Next steps"),(0,r.kt)("p",null,"That's it, we are ready to go. Let's start by ",(0,r.kt)("a",{parentName:"p",href:"infer-schema"},"loading some data")," into our warehouse and then ",(0,r.kt)("a",{parentName:"p",href:"transform"},"transforming it")," to make it ready for analysis."),(0,r.kt)("admonition",{type:"note"},(0,r.kt)("p",{parentName:"admonition"},"In a real world, your data would be stored in a database and not in your local filesystem."),(0,r.kt)("p",{parentName:"admonition"},"The example above is just to show you how to get started quickly."),(0,r.kt)("p",{parentName:"admonition"},"The extract, load and transform steps are independent and you could skip any of them if you don't need to extract, load or transform data.")))}f.isMDXComponent=!0}}]);