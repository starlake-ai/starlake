"use strict";(self.webpackChunkstarlake=self.webpackChunkstarlake||[]).push([[186],{3905:(e,t,a)=>{a.d(t,{Zo:()=>c,kt:()=>k});var n=a(67294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function l(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function o(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?l(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):l(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function s(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},l=Object.keys(e);for(n=0;n<l.length;n++)a=l[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(n=0;n<l.length;n++)a=l[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var i=n.createContext({}),u=function(e){var t=n.useContext(i),a=t;return e&&(a="function"==typeof e?e(t):o(o({},t),e)),a},c=function(e){var t=u(e.components);return n.createElement(i.Provider,{value:t},e.children)},p="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,l=e.originalType,i=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),p=u(a),m=r,k=p["".concat(i,".").concat(m)]||p[m]||d[m]||l;return a?n.createElement(k,o(o({ref:t},c),{},{components:a})):n.createElement(k,o({ref:t},c))}));function k(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var l=a.length,o=new Array(l);o[0]=m;var s={};for(var i in t)hasOwnProperty.call(t,i)&&(s[i]=t[i]);s.originalType=e,s[p]="string"==typeof e?e:r,o[1]=s;for(var u=2;u<l;u++)o[u]=a[u];return n.createElement.apply(null,o)}return n.createElement.apply(null,a)}m.displayName="MDXCreateElement"},85162:(e,t,a)=>{a.d(t,{Z:()=>o});var n=a(67294),r=a(86010);const l={tabItem:"tabItem_Ymn6"};function o(e){let{children:t,hidden:a,className:o}=e;return n.createElement("div",{role:"tabpanel",className:(0,r.Z)(l.tabItem,o),hidden:a},t)}},74866:(e,t,a)=>{a.d(t,{Z:()=>w});var n=a(87462),r=a(67294),l=a(86010),o=a(12466),s=a(16550),i=a(91980),u=a(67392),c=a(50012);function p(e){return function(e){return r.Children.map(e,(e=>{if(!e||(0,r.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)}))?.filter(Boolean)??[]}(e).map((e=>{let{props:{value:t,label:a,attributes:n,default:r}}=e;return{value:t,label:a,attributes:n,default:r}}))}function d(e){const{values:t,children:a}=e;return(0,r.useMemo)((()=>{const e=t??p(a);return function(e){const t=(0,u.l)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error(`Docusaurus error: Duplicate values "${t.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`)}(e),e}),[t,a])}function m(e){let{value:t,tabValues:a}=e;return a.some((e=>e.value===t))}function k(e){let{queryString:t=!1,groupId:a}=e;const n=(0,s.k6)(),l=function(e){let{queryString:t=!1,groupId:a}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!a)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return a??null}({queryString:t,groupId:a});return[(0,i._X)(l),(0,r.useCallback)((e=>{if(!l)return;const t=new URLSearchParams(n.location.search);t.set(l,e),n.replace({...n.location,search:t.toString()})}),[l,n])]}function h(e){const{defaultValue:t,queryString:a=!1,groupId:n}=e,l=d(e),[o,s]=(0,r.useState)((()=>function(e){let{defaultValue:t,tabValues:a}=e;if(0===a.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!m({value:t,tabValues:a}))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${t}" but none of its children has the corresponding value. Available values are: ${a.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);return t}const n=a.find((e=>e.default))??a[0];if(!n)throw new Error("Unexpected error: 0 tabValues");return n.value}({defaultValue:t,tabValues:l}))),[i,u]=k({queryString:a,groupId:n}),[p,h]=function(e){let{groupId:t}=e;const a=function(e){return e?`docusaurus.tab.${e}`:null}(t),[n,l]=(0,c.Nk)(a);return[n,(0,r.useCallback)((e=>{a&&l.set(e)}),[a,l])]}({groupId:n}),b=(()=>{const e=i??p;return m({value:e,tabValues:l})?e:null})();(0,r.useLayoutEffect)((()=>{b&&s(b)}),[b]);return{selectedValue:o,selectValue:(0,r.useCallback)((e=>{if(!m({value:e,tabValues:l}))throw new Error(`Can't select invalid tab value=${e}`);s(e),u(e),h(e)}),[u,h,l]),tabValues:l}}var b=a(72389);const f={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"};function g(e){let{className:t,block:a,selectedValue:s,selectValue:i,tabValues:u}=e;const c=[],{blockElementScrollPositionUntilNextRender:p}=(0,o.o5)(),d=e=>{const t=e.currentTarget,a=c.indexOf(t),n=u[a].value;n!==s&&(p(t),i(n))},m=e=>{let t=null;switch(e.key){case"Enter":d(e);break;case"ArrowRight":{const a=c.indexOf(e.currentTarget)+1;t=c[a]??c[0];break}case"ArrowLeft":{const a=c.indexOf(e.currentTarget)-1;t=c[a]??c[c.length-1];break}}t?.focus()};return r.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,l.Z)("tabs",{"tabs--block":a},t)},u.map((e=>{let{value:t,label:a,attributes:o}=e;return r.createElement("li",(0,n.Z)({role:"tab",tabIndex:s===t?0:-1,"aria-selected":s===t,key:t,ref:e=>c.push(e),onKeyDown:m,onClick:d},o,{className:(0,l.Z)("tabs__item",f.tabItem,o?.className,{"tabs__item--active":s===t})}),a??t)})))}function v(e){let{lazy:t,children:a,selectedValue:n}=e;const l=(Array.isArray(a)?a:[a]).filter(Boolean);if(t){const e=l.find((e=>e.props.value===n));return e?(0,r.cloneElement)(e,{className:"margin-top--md"}):null}return r.createElement("div",{className:"margin-top--md"},l.map(((e,t)=>(0,r.cloneElement)(e,{key:t,hidden:e.props.value!==n}))))}function y(e){const t=h(e);return r.createElement("div",{className:(0,l.Z)("tabs-container",f.tabList)},r.createElement(g,(0,n.Z)({},e,t)),r.createElement(v,(0,n.Z)({},e,t)))}function w(e){const t=(0,b.Z)();return r.createElement(y,(0,n.Z)({key:String(t)},e))}},17274:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>c,contentTitle:()=>i,default:()=>k,frontMatter:()=>s,metadata:()=>u,toc:()=>p});var n=a(87462),r=(a(67294),a(3905)),l=a(74866),o=a(85162);const s={},i="Installation",u={unversionedId:"install",id:"install",title:"Installation",description:"Starlake CLI",source:"@site/docs/010.install.mdx",sourceDirName:".",slug:"/install",permalink:"/starlake/docs/next/install",draft:!1,editUrl:"https://github.com/starlake-ai/starlake/edit/master/docs/docs/010.install.mdx",tags:[],version:"current",sidebarPosition:10,frontMatter:{},sidebar:"starlakeSidebar",previous:{title:"What is Starlake ?",permalink:"/starlake/docs/next/intro"},next:{title:"Guides",permalink:"/starlake/docs/next/category/guides"}},c={},p=[{value:"Starlake CLI",id:"starlake-cli",level:2},{value:"Prerequisites",id:"prerequisites",level:2},{value:"Install Starlake",id:"install-starlake",level:2},{value:"Graph Visualization",id:"graph-visualization",level:2},{value:"VS Code extension",id:"vs-code-extension",level:2}],d={toc:p},m="wrapper";function k(e){let{components:t,...a}=e;return(0,r.kt)(m,(0,n.Z)({},d,a,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"installation"},"Installation"),(0,r.kt)("h2",{id:"starlake-cli"},"Starlake CLI"),(0,r.kt)("h2",{id:"prerequisites"},"Prerequisites"),(0,r.kt)("p",null,"Make sure you have Java 11+ installed on your machine."),(0,r.kt)("p",null,"You can check your Java version by typing ",(0,r.kt)("inlineCode",{parentName:"p"},"java -version")," in a terminal."),(0,r.kt)("p",null,"If you don't have Java 11+ installed, you can download it from\n",(0,r.kt)("a",{parentName:"p",href:"https://www.oracle.com/java/technologies/javase-jdk11-downloads.html"},"Oracle JDK")," or ",(0,r.kt)("a",{parentName:"p",href:"https://adoptopenjdk.net/"},"OpenJDK")),(0,r.kt)("h2",{id:"install-starlake"},"Install Starlake"),(0,r.kt)("p",null,"To install starlake, you need to download the setup script from github.\nThe script will in turn download required dependencies and copy them to the bin subdirectory."),(0,r.kt)(l.Z,{groupId:"platforms",mdxType:"Tabs"},(0,r.kt)(o.Z,{value:"linux_macos",label:"Linux/MacOS",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"sh <(curl https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/setup.sh)\n"))),(0,r.kt)(o.Z,{value:"windows",label:"Windows",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-powershell"},'Invoke-Expression (Invoke-WebRequest -Uri "https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/setup.ps1").Content\n'))),(0,r.kt)(o.Z,{value:"docker",label:"Docker",mdxType:"TabItem"},(0,r.kt)("p",null,"Pull the docker image from the ",(0,r.kt)("a",{parentName:"p",href:"https://hub.docker.com/repository/docker/starlakeai/starlake/tags?page=1&ordering=last_updated"},"docker hub")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ docker pull starlakeai/starlake:1.2.0\n")),(0,r.kt)("p",null,"You may also run Starlake from a docker container. To do so, download this ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/starlake-ai/starlake/blob/master/distrib/Dockerfile"},"Dockerfile"),"\nand build your docker image"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ git clone git@github.com:starlake-ai/starlake.git\n$ cd starlake\n$ docker build -t starlakeai/starlake  .\n$ docker run -it starlake help\n\n")),(0,r.kt)("p",null,"To build the docker image with a specific version of Starlake or a specific branch, you can use the following command:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-sh"},"$ docker build -t starlakeai/starlake:1.2.0 --build-arg SL_VERSION=1.2.0 .\n")),(0,r.kt)("p",null,"To execute the docker image, you can use the following command:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ docker run -it -v /path/to/starlake/project:/starlake starlake <command>\n\n")),(0,r.kt)("p",null,"Note that you can pass environment variables to the docker image to configure the CLI. For example, to run against AWS redshift, you can pass the following environment variables:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},'\n$ SL_ROOT="s3a://my-bucket/my-starlake-project-base-dir/"\n$ docker run -e SL_ROOT=$SL_ROOT \\\n    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \\\n    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \\\n    -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN \\\n    -e REDSHIFT_PASSWORD=$REDSHIFT_PASSWORD \\\n    -it starlake <command>\n\n')))),(0,r.kt)("p",null,"The following folders should now have been created and contain Starlake dependencies."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-text"},"\nstarlake\n\u2514\u2500\u2500 bin\n    \u251c\u2500\u2500 deps\n    \u251c\u2500\u2500 sl\n    \u2514\u2500\u2500 spark\n\n")),(0,r.kt)("admonition",{type:"note"},(0,r.kt)("p",{parentName:"admonition"},"Any extra library you may need (Oracle client for example) need to be copied in the bin/deps folder.")),(0,r.kt)("p",null,"Starlake is now installed with all its dependencies. You can run the CLI by typing ",(0,r.kt)("inlineCode",{parentName:"p"},"starlake"),"."),(0,r.kt)("p",null,"This will display the commands supported by the CLI."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"Starlake Version 1.2.0\nUsage:\n    starlake [command]\nAvailable commands =>\n    lineage\n    bootstrap\n    bq2yml or bq-info\n    compare\n    cnxload\n    esload\n    extract-data\n    extract-schema\n    import\n    infer-schema\n    kafkaload\n    load\n    metrics\n    parquet2csv\n    transform\n    watch\n    xls2yml\n    yml2ddl\n    table-dependencies\n    yml2xls\n")),(0,r.kt)("p",null,"That's it! We now need to ",(0,r.kt)("a",{parentName:"p",href:"../user-guide/bootstrap"},"bootstrap a new project"),"."),(0,r.kt)("h2",{id:"graph-visualization"},"Graph Visualization"),(0,r.kt)("p",null,"Starlake provides features to visualize the lineage of a table,\nthe relationship between tables, and table level and row level acess policies."),(0,r.kt)("p",null,"To use these features, you need to install the ",(0,r.kt)("a",{parentName:"p",href:"https://graphviz.org/download/"},"GraphViz")," on top of which the starlake graph generator is built."),(0,r.kt)(l.Z,{groupId:"graphviz",mdxType:"Tabs"},(0,r.kt)(o.Z,{value:"linux",label:"Linux",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"sudo [apt|yum] install graphviz\n"))),(0,r.kt)(o.Z,{value:"macos",label:"MacOS",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"brew install graphviz\n"))),(0,r.kt)(o.Z,{value:"windows",label:"Windows",mdxType:"TabItem"},(0,r.kt)("p",null,"Download and install one of the packages from ",(0,r.kt)("a",{parentName:"p",href:"https://graphviz.org/download/#windows"},"GraphViz"),".")),(0,r.kt)(o.Z,{value:"docker",label:"Docker",mdxType:"TabItem"},(0,r.kt)("p",null,"GraphViz comes pre-installed with the starlake docker image."))),(0,r.kt)("h2",{id:"vs-code-extension"},"VS Code extension"),(0,r.kt)("p",null,"Starlake comes with a vs-code plugin that allows you to interact with the Starlake CLI.\nYou can install it from the ",(0,r.kt)("a",{parentName:"p",href:"https://marketplace.visualstudio.com/items?itemName=starlake.starlake"},"vs-code marketplace"),"."))}k.isMDXComponent=!0}}]);