"use strict";(self.webpackChunkstarlake=self.webpackChunkstarlake||[]).push([[3626],{3905:(e,t,a)=>{a.d(t,{Zo:()=>c,kt:()=>g});var r=a(67294);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function i(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?i(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):i(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function o(e,t){if(null==e)return{};var a,r,n=function(e,t){if(null==e)return{};var a,r,n={},i=Object.keys(e);for(r=0;r<i.length;r++)a=i[r],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)a=i[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var s=r.createContext({}),d=function(e){var t=r.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},c=function(e){var t=d(e.components);return r.createElement(s.Provider,{value:t},e.children)},p="mdxType",m={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},u=r.forwardRef((function(e,t){var a=e.components,n=e.mdxType,i=e.originalType,s=e.parentName,c=o(e,["components","mdxType","originalType","parentName"]),p=d(a),u=n,g=p["".concat(s,".").concat(u)]||p[u]||m[u]||i;return a?r.createElement(g,l(l({ref:t},c),{},{components:a})):r.createElement(g,l({ref:t},c))}));function g(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var i=a.length,l=new Array(i);l[0]=u;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o[p]="string"==typeof e?e:n,l[1]=o;for(var d=2;d<i;d++)l[d]=a[d];return r.createElement.apply(null,l)}return r.createElement.apply(null,a)}u.displayName="MDXCreateElement"},20312:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>s,contentTitle:()=>l,default:()=>m,frontMatter:()=>i,metadata:()=>o,toc:()=>d});var r=a(87462),n=(a(67294),a(3905));const i={},l="Load strategies",o={unversionedId:"guides/load/load-strategies",id:"guides/load/load-strategies",title:"Load strategies",description:"Standard strategies",source:"@site/docs/0300-guides/200-load/155-load-strategies.mdx",sourceDirName:"0300-guides/200-load",slug:"/guides/load/load-strategies",permalink:"/starlake/docs/next/guides/load/load-strategies",draft:!1,editUrl:"https://github.com/starlake-ai/starlake/edit/master/docs/docs/0300-guides/200-load/155-load-strategies.mdx",tags:[],version:"current",sidebarPosition:155,frontMatter:{},sidebar:"starlakeSidebar",previous:{title:"Load fixed width files",permalink:"/starlake/docs/next/guides/load/position"},next:{title:"Write strategies",permalink:"/starlake/docs/next/guides/load/write-strategies"}},s={},d=[{value:"Standard strategies",id:"standard-strategies",level:2},{value:"Custom Strategies",id:"custom-strategies",level:2}],c={toc:d},p="wrapper";function m(e){let{components:t,...a}=e;return(0,n.kt)(p,(0,r.Z)({},c,a,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"load-strategies"},"Load strategies"),(0,n.kt)("h2",{id:"standard-strategies"},"Standard strategies"),(0,n.kt)("p",null,"The ",(0,n.kt)("inlineCode",{parentName:"p"},"starlake")," load  will look for each domain and table, the files that match the pattern specified in\nthe ",(0,n.kt)("inlineCode",{parentName:"p"},"table.pattern")," attribute of the ",(0,n.kt)("inlineCode",{parentName:"p"},"metadata/load/<domain>/<table>.sl.yml")," file in the directory specified in the ",(0,n.kt)("inlineCode",{parentName:"p"},"load.metadata.directory"),"\nattribute of the same file or, if not specified, from the ",(0,n.kt)("inlineCode",{parentName:"p"},"<domain>/_config.sl.yml")," file."),(0,n.kt)("p",null,"starlake comes with two load strategies:"),(0,n.kt)("table",null,(0,n.kt)("thead",{parentName:"table"},(0,n.kt)("tr",{parentName:"thead"},(0,n.kt)("th",{parentName:"tr",align:null},"Load Strategy"),(0,n.kt)("th",{parentName:"tr",align:null},"Description"))),(0,n.kt)("tbody",{parentName:"table"},(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},(0,n.kt)("inlineCode",{parentName:"td"},"ai.starlake.job.load.IngestionTimeStrategy")),(0,n.kt)("td",{parentName:"tr",align:null},"Load the files in a chronological order based on the file last modification time. This is the default.")),(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},(0,n.kt)("inlineCode",{parentName:"td"},"ai.starlake.job.load.IngestionNameStrategy")),(0,n.kt)("td",{parentName:"tr",align:null},"Load the files in a lexicographical order based on the file name.")))),(0,n.kt)("p",null,"To use a load strategy, you need to specify the ",(0,n.kt)("inlineCode",{parentName:"p"},"loadStrategyClass")," attribute in the ",(0,n.kt)("inlineCode",{parentName:"p"},"metadata/application.sl.yml")," file."),(0,n.kt)("br",null),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-yaml",metastring:'title="metadata/application.sl.yml: to switch from a time based load to a name based load"',title:'"metadata/application.sl.yml:',to:!0,switch:!0,from:!0,a:!0,time:!0,based:!0,load:!0,name:!0,'load"':!0},"application:\n  ...\n  loadStrategyClass: ai.starlake.job.load.IngestionNameStrategy\n  ...\n")),(0,n.kt)("h2",{id:"custom-strategies"},"Custom Strategies"),(0,n.kt)("p",null,"You can define your own load strategy by implementing the ",(0,n.kt)("inlineCode",{parentName:"p"},"ai.starlake.job.load.LoadStrategy")," interface."),(0,n.kt)("br",null),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-scala",metastring:'title="src/main/scala/my/own//CustomLoadStrategy.scala"',title:'"src/main/scala/my/own//CustomLoadStrategy.scala"'},'object CustomLoadStrategy extends LoadStrategy with StrictLogging {\n\n  def list(\n    storageHandler: StorageHandler,\n    path: Path,\n    extension: String = "",\n    since: LocalDateTime = LocalDateTime.MIN,\n    recursive: Boolean\n  ): List[FileInfo] = ???\n}\n')),(0,n.kt)("br",null),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-yaml",metastring:'title="metadata/application.sl.yml: to use a custom load strategy"',title:'"metadata/application.sl.yml:',to:!0,use:!0,a:!0,custom:!0,load:!0,'strategy"':!0},"\napplication:\n  ...\n  loadStrategyClass: ai.starlake.job.load.MyLoadStrategy\n  ...\n")))}m.isMDXComponent=!0}}]);