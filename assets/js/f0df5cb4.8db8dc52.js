"use strict";(self.webpackChunkstarlake=self.webpackChunkstarlake||[]).push([[8160],{3905:(e,t,r)=>{r.d(t,{Zo:()=>p,kt:()=>f});var n=r(67294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var s=n.createContext({}),c=function(e){var t=n.useContext(s),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},p=function(e){var t=c(e.components);return n.createElement(s.Provider,{value:t},e.children)},d="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,i=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),d=c(r),m=a,f=d["".concat(s,".").concat(m)]||d[m]||u[m]||i;return r?n.createElement(f,o(o({ref:t},p),{},{components:r})):n.createElement(f,o({ref:t},p))}));function f(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=r.length,o=new Array(i);o[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[d]="string"==typeof e?e:a,o[1]=l;for(var c=2;c<i;c++)o[c]=r[c];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},92294:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>s,contentTitle:()=>o,default:()=>u,frontMatter:()=>i,metadata:()=>l,toc:()=>c});var n=r(87462),a=(r(67294),r(3905));const i={sidebar_position:280,title:"stage"},o=void 0,l={unversionedId:"cli/stage",id:"cli/stage",title:"stage",description:"Synopsis",source:"@site/docs/0800-cli/stage.md",sourceDirName:"0800-cli",slug:"/cli/stage",permalink:"/starlake/docs/next/cli/stage",draft:!1,editUrl:"https://github.com/starlake-ai/starlake/edit/master/docs/docs/0800-cli/stage.md",tags:[],version:"current",sidebarPosition:280,frontMatter:{sidebar_position:280,title:"stage"},sidebar:"starlakeSidebar",previous:{title:"site",permalink:"/starlake/docs/next/cli/site"},next:{title:"table-dependencies",permalink:"/starlake/docs/next/cli/table-dependencies"}},s={},c=[{value:"Synopsis",id:"synopsis",level:2},{value:"Description",id:"description",level:2},{value:"Parameters",id:"parameters",level:2}],p={toc:c},d="wrapper";function u(e){let{components:t,...r}=e;return(0,a.kt)(d,(0,n.Z)({},p,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h2",{id:"synopsis"},"Synopsis"),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"starlake stage ","[options]")),(0,a.kt)("h2",{id:"description"},"Description"),(0,a.kt)("p",null,"Move the files from the landing area to the pending area."),(0,a.kt)("p",null,"Files are loaded one domain at a time."),(0,a.kt)("p",null,'Each domain has its own directory and is specified in the "directory" key of Domain YML file\ncompressed files are uncompressed if a corresponding ack file exist.'),(0,a.kt)("p",null,"Compressed files are recognized by their extension which should be one of .tgz, .zip, .gz.\nraw file should also have a corresponding ack file\nbefore moving the files to the pending area, the ack files are deleted."),(0,a.kt)("p",null,'To import files without ack specify an empty "ack" key (aka ack:"") in the domain YML file.'),(0,a.kt)("p",null,'"ack" is the default ack extension searched for but you may specify a different one in the domain YML file.'),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"comet import\n")),(0,a.kt)("h2",{id:"parameters"},"Parameters"),(0,a.kt)("table",null,(0,a.kt)("thead",{parentName:"table"},(0,a.kt)("tr",{parentName:"thead"},(0,a.kt)("th",{parentName:"tr",align:null},"Parameter"),(0,a.kt)("th",{parentName:"tr",align:null},"Cardinality"),(0,a.kt)("th",{parentName:"tr",align:null},"Description"))),(0,a.kt)("tbody",{parentName:"table"},(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"--domains:",(0,a.kt)("inlineCode",{parentName:"td"},"domain1,domain2...")),(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("em",{parentName:"td"},"Optional")),(0,a.kt)("td",{parentName:"tr",align:null},"Domains to stage")))))}u.isMDXComponent=!0}}]);