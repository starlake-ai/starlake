"use strict";(self.webpackChunkstarlake=self.webpackChunkstarlake||[]).push([[7238],{3905:(e,t,a)=>{a.d(t,{Zo:()=>u,kt:()=>M});var i=a(67294);function l(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function n(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);t&&(i=i.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,i)}return a}function r(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?n(Object(a),!0).forEach((function(t){l(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):n(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function c(e,t){if(null==e)return{};var a,i,l=function(e,t){if(null==e)return{};var a,i,l={},n=Object.keys(e);for(i=0;i<n.length;i++)a=n[i],t.indexOf(a)>=0||(l[a]=e[a]);return l}(e,t);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);for(i=0;i<n.length;i++)a=n[i],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(l[a]=e[a])}return l}var o=i.createContext({}),s=function(e){var t=i.useContext(o),a=t;return e&&(a="function"==typeof e?e(t):r(r({},t),e)),a},u=function(e){var t=s(e.components);return i.createElement(o.Provider,{value:t},e.children)},d="mdxType",m={inlineCode:"code",wrapper:function(e){var t=e.children;return i.createElement(i.Fragment,{},t)}},I=i.forwardRef((function(e,t){var a=e.components,l=e.mdxType,n=e.originalType,o=e.parentName,u=c(e,["components","mdxType","originalType","parentName"]),d=s(a),I=l,M=d["".concat(o,".").concat(I)]||d[I]||m[I]||n;return a?i.createElement(M,r(r({ref:t},u),{},{components:a})):i.createElement(M,r({ref:t},u))}));function M(e,t){var a=arguments,l=t&&t.mdxType;if("string"==typeof e||l){var n=a.length,r=new Array(n);r[0]=I;var c={};for(var o in t)hasOwnProperty.call(t,o)&&(c[o]=t[o]);c.originalType=e,c[d]="string"==typeof e?e:l,r[1]=c;for(var s=2;s<n;s++)r[s]=a[s];return i.createElement.apply(null,r)}return i.createElement.apply(null,a)}I.displayName="MDXCreateElement"},85162:(e,t,a)=>{a.d(t,{Z:()=>r});var i=a(67294),l=a(86010);const n={tabItem:"tabItem_Ymn6"};function r(e){let{children:t,hidden:a,className:r}=e;return i.createElement("div",{role:"tabpanel",className:(0,l.Z)(n.tabItem,r),hidden:a},t)}},74866:(e,t,a)=>{a.d(t,{Z:()=>T});var i=a(87462),l=a(67294),n=a(86010),r=a(12466),c=a(16550),o=a(91980),s=a(67392),u=a(50012);function d(e){return function(e){return l.Children.map(e,(e=>{if(!e||(0,l.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)}))?.filter(Boolean)??[]}(e).map((e=>{let{props:{value:t,label:a,attributes:i,default:l}}=e;return{value:t,label:a,attributes:i,default:l}}))}function m(e){const{values:t,children:a}=e;return(0,l.useMemo)((()=>{const e=t??d(a);return function(e){const t=(0,s.l)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error(`Docusaurus error: Duplicate values "${t.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`)}(e),e}),[t,a])}function I(e){let{value:t,tabValues:a}=e;return a.some((e=>e.value===t))}function M(e){let{queryString:t=!1,groupId:a}=e;const i=(0,c.k6)(),n=function(e){let{queryString:t=!1,groupId:a}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!a)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return a??null}({queryString:t,groupId:a});return[(0,o._X)(n),(0,l.useCallback)((e=>{if(!n)return;const t=new URLSearchParams(i.location.search);t.set(n,e),i.replace({...i.location,search:t.toString()})}),[n,i])]}function b(e){const{defaultValue:t,queryString:a=!1,groupId:i}=e,n=m(e),[r,c]=(0,l.useState)((()=>function(e){let{defaultValue:t,tabValues:a}=e;if(0===a.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!I({value:t,tabValues:a}))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${t}" but none of its children has the corresponding value. Available values are: ${a.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);return t}const i=a.find((e=>e.default))??a[0];if(!i)throw new Error("Unexpected error: 0 tabValues");return i.value}({defaultValue:t,tabValues:n}))),[o,s]=M({queryString:a,groupId:i}),[d,b]=function(e){let{groupId:t}=e;const a=function(e){return e?`docusaurus.tab.${e}`:null}(t),[i,n]=(0,u.Nk)(a);return[i,(0,l.useCallback)((e=>{a&&n.set(e)}),[a,n])]}({groupId:i}),g=(()=>{const e=o??d;return I({value:e,tabValues:n})?e:null})();(0,l.useLayoutEffect)((()=>{g&&c(g)}),[g]);return{selectedValue:r,selectValue:(0,l.useCallback)((e=>{if(!I({value:e,tabValues:n}))throw new Error(`Can't select invalid tab value=${e}`);c(e),s(e),b(e)}),[s,b,n]),tabValues:n}}var g=a(72389);const N={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"};function Z(e){let{className:t,block:a,selectedValue:c,selectValue:o,tabValues:s}=e;const u=[],{blockElementScrollPositionUntilNextRender:d}=(0,r.o5)(),m=e=>{const t=e.currentTarget,a=u.indexOf(t),i=s[a].value;i!==c&&(d(t),o(i))},I=e=>{let t=null;switch(e.key){case"Enter":m(e);break;case"ArrowRight":{const a=u.indexOf(e.currentTarget)+1;t=u[a]??u[0];break}case"ArrowLeft":{const a=u.indexOf(e.currentTarget)-1;t=u[a]??u[u.length-1];break}}t?.focus()};return l.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,n.Z)("tabs",{"tabs--block":a},t)},s.map((e=>{let{value:t,label:a,attributes:r}=e;return l.createElement("li",(0,i.Z)({role:"tab",tabIndex:c===t?0:-1,"aria-selected":c===t,key:t,ref:e=>u.push(e),onKeyDown:I,onClick:m},r,{className:(0,n.Z)("tabs__item",N.tabItem,r?.className,{"tabs__item--active":c===t})}),a??t)})))}function p(e){let{lazy:t,children:a,selectedValue:i}=e;const n=(Array.isArray(a)?a:[a]).filter(Boolean);if(t){const e=n.find((e=>e.props.value===i));return e?(0,l.cloneElement)(e,{className:"margin-top--md"}):null}return l.createElement("div",{className:"margin-top--md"},n.map(((e,t)=>(0,l.cloneElement)(e,{key:t,hidden:e.props.value!==i}))))}function w(e){const t=b(e);return l.createElement("div",{className:(0,n.Z)("tabs-container",N.tabList)},l.createElement(Z,(0,i.Z)({},e,t)),l.createElement(p,(0,i.Z)({},e,t)))}function T(e){const t=(0,g.Z)();return l.createElement(w,(0,i.Z)({key:String(t)},e))}},85978:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>o,contentTitle:()=>r,default:()=>m,frontMatter:()=>n,metadata:()=>c,toc:()=>s});var i=a(87462),l=(a(67294),a(3905));a(74866),a(85162);const n={},r="Orchestration",c={unversionedId:"user-guide/orchestration",id:"version-1.0.0/user-guide/orchestration",title:"Orchestration",description:"Now that we have seen how to load and transform data, we will see how to orchestrate the tasks using the starlake command line tool.",source:"@site/versioned_docs/version-1.0.0/0400-user-guide/350-orchestration.mdx",sourceDirName:"0400-user-guide",slug:"/user-guide/orchestration",permalink:"/starlake/docs/1.0.0/user-guide/orchestration",draft:!1,editUrl:"https://github.com/starlake-ai/starlake/edit/master/docs/versioned_docs/version-1.0.0/0400-user-guide/350-orchestration.mdx",tags:[],version:"1.0.0",sidebarPosition:350,frontMatter:{},sidebar:"starlakeSidebar",previous:{title:"Access Control",permalink:"/starlake/docs/1.0.0/user-guide/security"},next:{title:"About Metrics",permalink:"/starlake/docs/1.0.0/user-guide/metrics"}},o={},s=[{value:"Data loading",id:"data-loading",level:2},{value:"Data transformation",id:"data-transformation",level:2}],u={toc:s},d="wrapper";function m(e){let{components:t,...n}=e;return(0,l.kt)(d,(0,i.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,l.kt)("h1",{id:"orchestration"},"Orchestration"),(0,l.kt)("p",null,"Now that we have seen how to load and transform data, we will see how to orchestrate the tasks using the ",(0,l.kt)("inlineCode",{parentName:"p"},"starlake")," command line tool.\nstarlake is not an orchestration tool, but it can be used to generate your DAG based on templates and to run your transforms in the right order on your tools of choice for scheduling and monitoring batch oriented workflows."),(0,l.kt)("h2",{id:"data-loading"},"Data loading"),(0,l.kt)("p",null,"Templates may be defined in the following directories:"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"an absolute path name"),(0,l.kt)("li",{parentName:"ul"},"a relative path name to the ",(0,l.kt)("inlineCode",{parentName:"li"},"metadata/dags/templates/")," directory"),(0,l.kt)("li",{parentName:"ul"},"a relative path name to the ",(0,l.kt)("inlineCode",{parentName:"li"},"src/main/resources/templates/dags/")," resource directory")),(0,l.kt)("p",null,"Below a few DAG templates available in the resource directory:"),(0,l.kt)("table",null,(0,l.kt)("thead",{parentName:"table"},(0,l.kt)("tr",{parentName:"thead"},(0,l.kt)("th",{parentName:"tr",align:null},"DAG Template"),(0,l.kt)("th",{parentName:"tr",align:null},"Description"))),(0,l.kt)("tbody",{parentName:"table"},(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"load/schedule_table_bash.py.j2")),(0,l.kt)("td",{parentName:"tr",align:null},"This template executes individual bash jobs and requires the following dag generation options set: ",(0,l.kt)("br",null)," - SL_ROOT: The root project path ",(0,l.kt)("br",null)," - SL_STARLAKE_PATH: the path to the starlake executable")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"load/schedule_table_cloudrun.py.j2")),(0,l.kt)("td",{parentName:"tr",align:null},"This template executes individual cloud run jobs and requires the following dag generation options set: ",(0,l.kt)("br",null)," - SL_ROOT: The root project path ",(0,l.kt)("br",null)," - SL_STARLAKE_PATH: the path to the starlake executable")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"load/sensor_table_cloudrun_with_ack.py.j2")),(0,l.kt)("td",{parentName:"tr",align:null},"This template executes individual cloud rub jobs upon ack file detection and requires the following dag generation options set: ",(0,l.kt)("br",null)," - SL_ROOT: The root project path ",(0,l.kt)("br",null)," - SL_STARLAKE_PATH: the path to the starlake executable")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"load/single_scheduled_tasks_with_wait_sensor.py.j2")),(0,l.kt)("td",{parentName:"tr",align:null},"This template executes individual cloud run upon file detection and requires the following dag generation options set: ",(0,l.kt)("br",null)," - SL_ROOT: The root project path ",(0,l.kt)("br",null)," - SL_STARLAKE_PATH: the path to the starlake executable")))),(0,l.kt)("p",null,"You may define the DAG template at the following level:"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"at the project level: In the application configuration file ",(0,l.kt)("inlineCode",{parentName:"li"},"application.sl.yml")," under the ",(0,l.kt)("inlineCode",{parentName:"li"},"dagRef.load")," property. In this case the template will be used as the default template for all the tables in the project."),(0,l.kt)("li",{parentName:"ul"},"at the domain level: In the ",(0,l.kt)("inlineCode",{parentName:"li"},"_config.sl.yml")," configuration file under the ",(0,l.kt)("inlineCode",{parentName:"li"},"metadata.dagRef")," property. In this case the template will be used as the default template for all the tables in the domain."),(0,l.kt)("li",{parentName:"ul"},"at the table level: In the ",(0,l.kt)("inlineCode",{parentName:"li"},"tablename.sl.yml")," configuration file under the ",(0,l.kt)("inlineCode",{parentName:"li"},"metadata.dagRef")," property. In this case the template will be used for the table only.")),(0,l.kt)("p",null,"You may define a custom DAG template using the ",(0,l.kt)("a",{parentName:"p",href:"https://hub.synerise.com/developers/inserts/tag/"},"Jinjava")," template engine\ndeveloped at ",(0,l.kt)("a",{parentName:"p",href:"https://github.com/HubSpot/jinjava"},"HubSpot"),"."),(0,l.kt)("h2",{id:"data-transformation"},"Data transformation"),(0,l.kt)("p",null,"In the same way, you may define DAG templates for the data transformation tasks. The default templates are located in the ",(0,l.kt)("inlineCode",{parentName:"p"},"src/main/resources/templates/dags/transform")," directory."),(0,l.kt)("p",null,"For transfmations, starlake may even generate a DAG for a transfmation task and all its dependencies.\nFor example, looking at starbake project, we can generate the DAG for the ",(0,l.kt)("inlineCode",{parentName:"p"},"Products.TopSellingProducts")," task and all its dependencies using the following command:"),(0,l.kt)("p",null,(0,l.kt)("img",{src:a(97648).Z,width:"1908",height:"344"})),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"$ starlake dag-generate --tasks Products.TopSellingProducts --output dag.py\n")),(0,l.kt)("p",null,"The resulting DAG is shown below:"),(0,l.kt)("p",null,(0,l.kt)("img",{src:a(56740).Z,width:"1509",height:"763"})))}m.isMDXComponent=!0},56740:(e,t,a)=>{a.d(t,{Z:()=>i});const i=a.p+"assets/images/transform-dags-7b3fd2a66f18b84d679a07f8893e6447.png"},97648:(e,t,a)=>{a.d(t,{Z:()=>i});const i="data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiIHN0YW5kYWxvbmU9Im5vIj8+PCFET0NUWVBFIHN2ZyBQVUJMSUMgIi0vL1czQy8vRFREIFNWRyAxLjEvL0VOIiAiaHR0cDovL3d3dy53My5vcmcvR3JhcGhpY3MvU1ZHLzEuMS9EVEQvc3ZnMTEuZHRkIj48IS0tIEdlbmVyYXRlZCBieSBncmFwaHZpeiB2ZXJzaW9uIDIuNDAuMSAoMjAxNjEyMjUuMDMwNCkKIC0tPjwhLS0gVGl0bGU6ICUwIFBhZ2VzOiAxIC0tPjxzdmcgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIiB4bWxuczp4bGluaz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94bGluayIgd2lkdGg9IjE0MzFwdCIgaGVpZ2h0PSIyNThwdCIgdmlld0JveD0iMC4wMCAwLjAwIDE0MzEuMDAgMjU4LjAwIj4KPGcgaWQ9ImdyYXBoMCIgY2xhc3M9ImdyYXBoIiB0cmFuc2Zvcm09InNjYWxlKDEgMSkgcm90YXRlKDApIHRyYW5zbGF0ZSgzNiAyMjIpIj4KPHRpdGxlPiUwPC90aXRsZT4KPHBvbHlnb24gZmlsbD0iI2ZmZmZmZiIgc3Ryb2tlPSJ0cmFuc3BhcmVudCIgcG9pbnRzPSItMzYsMzYgLTM2LC0yMjIgMTM5NSwtMjIyIDEzOTUsMzYgLTM2LDM2Ii8+CjwhLS0gUHJvZHVjdHNfUHJvZHVjdFByb2ZpdGFiaWxpdHkgLS0+CjxnIGlkPSJub2RlMSIgY2xhc3M9Im5vZGUiPgo8dGl0bGU+UHJvZHVjdHNfUHJvZHVjdFByb2ZpdGFiaWxpdHk8L3RpdGxlPgo8cG9seWdvbiBmaWxsPSIjMDAwMDhiIiBzdHJva2U9InRyYW5zcGFyZW50IiBwb2ludHM9IjgyMiwtNzQgODIyLC0xMTIgMTA0MCwtMTEyIDEwNDAsLTc0IDgyMiwtNzQiLz4KPHBvbHlnb24gZmlsbD0ibm9uZSIgc3Ryb2tlPSIjMDAwMDAwIiBwb2ludHM9IjgyMiwtNzQgODIyLC0xMTIgMTA0MCwtMTEyIDEwNDAsLTc0IDgyMiwtNzQiLz4KPHRleHQgdGV4dC1hbmNob3I9InN0YXJ0IiB4PSI4MzIuNjAxIiB5PSItODkuNCIgZm9udC1mYW1pbHk9IkFyaWFsIiBmb250LXdlaWdodD0iYm9sZCIgZm9udC1zaXplPSIxNC4wMCIgZmlsbD0iI2ZmZmZmZiI+IFByb2R1Y3RzLlByb2R1Y3RQcm9maXRhYmlsaXR5wqDCoDwvdGV4dD4KPC9nPgo8IS0tIHN0YXJiYWtlX1Byb2R1Y3RzIC0tPgo8ZyBpZD0ibm9kZTIiIGNsYXNzPSJub2RlIj4KPHRpdGxlPnN0YXJiYWtlX1Byb2R1Y3RzPC90aXRsZT4KPHBvbHlnb24gZmlsbD0iI2ZmZmZmZiIgc3Ryb2tlPSJ0cmFuc3BhcmVudCIgcG9pbnRzPSIxMTk3LjUsLTc0IDExOTcuNSwtMTEyIDEzNTIuNSwtMTEyIDEzNTIuNSwtNzQgMTE5Ny41LC03NCIvPgo8cG9seWdvbiBmaWxsPSJub25lIiBzdHJva2U9IiMwMDAwMDAiIHBvaW50cz0iMTE5Ny41LC03NCAxMTk3LjUsLTExMiAxMzUyLjUsLTExMiAxMzUyLjUsLTc0IDExOTcuNSwtNzQiLz4KPHRleHQgdGV4dC1hbmNob3I9InN0YXJ0IiB4PSIxMjA4LjA5OTYiIHk9Ii04OS40IiBmb250LWZhbWlseT0iQXJpYWwiIGZvbnQtd2VpZ2h0PSJib2xkIiBmb250LXNpemU9IjE0LjAwIiBmaWxsPSIjMDAwMDAwIj4gc3RhcmJha2UuUHJvZHVjdHPCoMKgPC90ZXh0Pgo8L2c+CjwhLS0gUHJvZHVjdHNfUHJvZHVjdFByb2ZpdGFiaWxpdHkmIzQ1OyZndDtzdGFyYmFrZV9Qcm9kdWN0cyAtLT4KPGcgaWQ9ImVkZ2UyIiBjbGFzcz0iZWRnZSI+Cjx0aXRsZT5Qcm9kdWN0c19Qcm9kdWN0UHJvZml0YWJpbGl0eS0mZ3Q7c3RhcmJha2VfUHJvZHVjdHM8L3RpdGxlPgo8cGF0aCBmaWxsPSJub25lIiBzdHJva2U9IiMwMDAwMDAiIGQ9Ik0xMDQwLjE1NzUsLTkzQzEwODcuNDUyLC05MyAxMTQyLjA4MjIsLTkzIDExODYuNjkyOSwtOTMiLz4KPHBvbHlnb24gZmlsbD0iIzAwMDAwMCIgc3Ryb2tlPSIjMDAwMDAwIiBwb2ludHM9IjExODYuOTI3MSwtOTYuNTAwMSAxMTk2LjkyNzEsLTkzIDExODYuOTI3LC04OS41MDAxIDExODYuOTI3MSwtOTYuNTAwMSIvPgo8L2c+CjwhLS0gc3RhcmJha2VfSW5ncmVkaWVudHMgLS0+CjxnIGlkPSJub2RlMyIgY2xhc3M9Im5vZGUiPgo8dGl0bGU+c3RhcmJha2VfSW5ncmVkaWVudHM8L3RpdGxlPgo8cG9seWdvbiBmaWxsPSIjZmZmZmZmIiBzdHJva2U9InRyYW5zcGFyZW50IiBwb2ludHM9IjExOTAuNSwwIDExOTAuNSwtMzggMTM1OS41LC0zOCAxMzU5LjUsMCAxMTkwLjUsMCIvPgo8cG9seWdvbiBmaWxsPSJub25lIiBzdHJva2U9IiMwMDAwMDAiIHBvaW50cz0iMTE5MC41LDAgMTE5MC41LC0zOCAxMzU5LjUsLTM4IDEzNTkuNSwwIDExOTAuNSwwIi8+Cjx0ZXh0IHRleHQtYW5jaG9yPSJzdGFydCIgeD0iMTIwMS4wOTU0IiB5PSItMTUuNCIgZm9udC1mYW1pbHk9IkFyaWFsIiBmb250LXdlaWdodD0iYm9sZCIgZm9udC1zaXplPSIxNC4wMCIgZmlsbD0iIzAwMDAwMCI+IHN0YXJiYWtlLkluZ3JlZGllbnRzwqDCoDwvdGV4dD4KPC9nPgo8IS0tIFByb2R1Y3RzX1Byb2R1Y3RQcm9maXRhYmlsaXR5JiM0NTsmZ3Q7c3RhcmJha2VfSW5ncmVkaWVudHMgLS0+CjxnIGlkPSJlZGdlMyIgY2xhc3M9ImVkZ2UiPgo8dGl0bGU+UHJvZHVjdHNfUHJvZHVjdFByb2ZpdGFiaWxpdHktJmd0O3N0YXJiYWtlX0luZ3JlZGllbnRzPC90aXRsZT4KPHBhdGggZmlsbD0ibm9uZSIgc3Ryb2tlPSIjMDAwMDAwIiBkPSJNMTAxOS40NTk4LC03My45NDMyQzEwNjguNjU4NCwtNjMuMzQ0MyAxMTI5Ljg4ODMsLTUwLjE1MzYgMTE4MC4wMjExLC0zOS4zNTM1Ii8+Cjxwb2x5Z29uIGZpbGw9IiMwMDAwMDAiIHN0cm9rZT0iIzAwMDAwMCIgcG9pbnRzPSIxMTgwLjg1NDMsLTQyLjc1NDQgMTE4OS44OTI5LC0zNy4yMjY5IDExNzkuMzgsLTM1LjkxMTQgMTE4MC44NTQzLC00Mi43NTQ0Ii8+CjwvZz4KPCEtLSBzdGFyYmFrZV9PcmRlcnMgLS0+CjxnIGlkPSJub2RlNiIgY2xhc3M9Im5vZGUiPgo8dGl0bGU+c3RhcmJha2VfT3JkZXJzPC90aXRsZT4KPHBvbHlnb24gZmlsbD0iI2ZmZmZmZiIgc3Ryb2tlPSJ0cmFuc3BhcmVudCIgcG9pbnRzPSIxMjAzLjUsLTE0OCAxMjAzLjUsLTE4NiAxMzQ2LjUsLTE4NiAxMzQ2LjUsLTE0OCAxMjAzLjUsLTE0OCIvPgo8cG9seWdvbiBmaWxsPSJub25lIiBzdHJva2U9IiMwMDAwMDAiIHBvaW50cz0iMTIwMy41LC0xNDggMTIwMy41LC0xODYgMTM0Ni41LC0xODYgMTM0Ni41LC0xNDggMTIwMy41LC0xNDgiLz4KPHRleHQgdGV4dC1hbmNob3I9InN0YXJ0IiB4PSIxMjE0LjMyODkiIHk9Ii0xNjMuNCIgZm9udC1mYW1pbHk9IkFyaWFsIiBmb250LXdlaWdodD0iYm9sZCIgZm9udC1zaXplPSIxNC4wMCIgZmlsbD0iIzAwMDAwMCI+IHN0YXJiYWtlLk9yZGVyc8KgwqA8L3RleHQ+CjwvZz4KPCEtLSBQcm9kdWN0c19Qcm9kdWN0UHJvZml0YWJpbGl0eSYjNDU7Jmd0O3N0YXJiYWtlX09yZGVycyAtLT4KPGcgaWQ9ImVkZ2UxIiBjbGFzcz0iZWRnZSI+Cjx0aXRsZT5Qcm9kdWN0c19Qcm9kdWN0UHJvZml0YWJpbGl0eS0mZ3Q7c3RhcmJha2VfT3JkZXJzPC90aXRsZT4KPHBhdGggZmlsbD0ibm9uZSIgc3Ryb2tlPSIjMDAwMDAwIiBkPSJNMTAxOS40NTk4LC0xMTIuMDU2OEMxMDcyLjk2MjcsLTEyMy41ODI5IDExNDAuNjk0MSwtMTM4LjE3NDMgMTE5Mi45MDg5LC0xNDkuNDIyOSIvPgo8cG9seWdvbiBmaWxsPSIjMDAwMDAwIiBzdHJva2U9IiMwMDAwMDAiIHBvaW50cz0iMTE5Mi4yOTAzLC0xNTIuODY5OCAxMjAyLjgwMzIsLTE1MS41NTQ0IDExOTMuNzY0NiwtMTQ2LjAyNjggMTE5Mi4yOTAzLC0xNTIuODY5OCIvPgo8L2c+CjwhLS0gUHJvZHVjdHNfUHJvZHVjdFBlcmZvcm1hbmNlIC0tPgo8ZyBpZD0ibm9kZTQiIGNsYXNzPSJub2RlIj4KPHRpdGxlPlByb2R1Y3RzX1Byb2R1Y3RQZXJmb3JtYW5jZTwvdGl0bGU+Cjxwb2x5Z29uIGZpbGw9IiMwMDAwOGIiIHN0cm9rZT0idHJhbnNwYXJlbnQiIHBvaW50cz0iODE2LC0xNDggODE2LC0xODYgMTA0NiwtMTg2IDEwNDYsLTE0OCA4MTYsLTE0OCIvPgo8cG9seWdvbiBmaWxsPSJub25lIiBzdHJva2U9IiMwMDAwMDAiIHBvaW50cz0iODE2LC0xNDggODE2LC0xODYgMTA0NiwtMTg2IDEwNDYsLTE0OCA4MTYsLTE0OCIvPgo8dGV4dCB0ZXh0LWFuY2hvcj0ic3RhcnQiIHg9IjgyNi43NjE2IiB5PSItMTYzLjQiIGZvbnQtZmFtaWx5PSJBcmlhbCIgZm9udC13ZWlnaHQ9ImJvbGQiIGZvbnQtc2l6ZT0iMTQuMDAiIGZpbGw9IiNmZmZmZmYiPiBQcm9kdWN0cy5Qcm9kdWN0UGVyZm9ybWFuY2XCoMKgPC90ZXh0Pgo8L2c+CjwhLS0gUHJvZHVjdHNfUHJvZHVjdFBlcmZvcm1hbmNlJiM0NTsmZ3Q7c3RhcmJha2VfUHJvZHVjdHMgLS0+CjxnIGlkPSJlZGdlNSIgY2xhc3M9ImVkZ2UiPgo8dGl0bGU+UHJvZHVjdHNfUHJvZHVjdFBlcmZvcm1hbmNlLSZndDtzdGFyYmFrZV9Qcm9kdWN0czwvdGl0bGU+CjxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iIzAwMDAwMCIgZD0iTTEwMTkuNDU5OCwtMTQ3Ljk0MzJDMTA3MC44OTQ3LC0xMzYuODYyNiAxMTM1LjQ3OTUsLTEyMi45NDkxIDExODYuNzg2NSwtMTExLjg5NjEiLz4KPHBvbHlnb24gZmlsbD0iIzAwMDAwMCIgc3Ryb2tlPSIjMDAwMDAwIiBwb2ludHM9IjExODcuODI0NCwtMTE1LjI1MjkgMTE5Ni44NjMsLTEwOS43MjUzIDExODYuMzUwMSwtMTA4LjQwOTggMTE4Ny44MjQ0LC0xMTUuMjUyOSIvPgo8L2c+CjwhLS0gUHJvZHVjdHNfUHJvZHVjdFBlcmZvcm1hbmNlJiM0NTsmZ3Q7c3RhcmJha2VfT3JkZXJzIC0tPgo8ZyBpZD0iZWRnZTQiIGNsYXNzPSJlZGdlIj4KPHRpdGxlPlByb2R1Y3RzX1Byb2R1Y3RQZXJmb3JtYW5jZS0mZ3Q7c3RhcmJha2VfT3JkZXJzPC90aXRsZT4KPHBhdGggZmlsbD0ibm9uZSIgc3Ryb2tlPSIjMDAwMDAwIiBkPSJNMTA0Ni4zMzYxLC0xNjdDMTA5NC4zNDM0LC0xNjcgMTE0OC45NjU5LC0xNjcgMTE5Mi43Mjc5LC0xNjciLz4KPHBvbHlnb24gZmlsbD0iIzAwMDAwMCIgc3Ryb2tlPSIjMDAwMDAwIiBwb2ludHM9IjExOTIuNzQ4NSwtMTcwLjUwMDEgMTIwMi43NDg1LC0xNjcgMTE5Mi43NDg0LC0xNjMuNTAwMSAxMTkyLjc0ODUsLTE3MC41MDAxIi8+CjwvZz4KPCEtLSBQcm9kdWN0c19Ub3BTZWxsaW5nUHJvZml0YWJsZVByb2R1Y3RzIC0tPgo8ZyBpZD0ibm9kZTUiIGNsYXNzPSJub2RlIj4KPHRpdGxlPlByb2R1Y3RzX1RvcFNlbGxpbmdQcm9maXRhYmxlUHJvZHVjdHM8L3RpdGxlPgo8cG9seWdvbiBmaWxsPSIjMDAwMDhiIiBzdHJva2U9InRyYW5zcGFyZW50IiBwb2ludHM9IjAsLTExMSAwLC0xNDkgMjgyLC0xNDkgMjgyLC0xMTEgMCwtMTExIi8+Cjxwb2x5Z29uIGZpbGw9Im5vbmUiIHN0cm9rZT0iIzAwMDAwMCIgcG9pbnRzPSIwLC0xMTEgMCwtMTQ5IDI4MiwtMTQ5IDI4MiwtMTExIDAsLTExMSIvPgo8dGV4dCB0ZXh0LWFuY2hvcj0ic3RhcnQiIHg9IjEwLjcwMDYiIHk9Ii0xMjYuNCIgZm9udC1mYW1pbHk9IkFyaWFsIiBmb250LXdlaWdodD0iYm9sZCIgZm9udC1zaXplPSIxNC4wMCIgZmlsbD0iI2ZmZmZmZiI+IFByb2R1Y3RzLlRvcFNlbGxpbmdQcm9maXRhYmxlUHJvZHVjdHPCoMKgPC90ZXh0Pgo8L2c+CjwhLS0gUHJvZHVjdHNfTW9zdFByb2ZpdGFibGVQcm9kdWN0cyAtLT4KPGcgaWQ9Im5vZGU3IiBjbGFzcz0ibm9kZSI+Cjx0aXRsZT5Qcm9kdWN0c19Nb3N0UHJvZml0YWJsZVByb2R1Y3RzPC90aXRsZT4KPHBvbHlnb24gZmlsbD0iIzAwMDA4YiIgc3Ryb2tlPSJ0cmFuc3BhcmVudCIgcG9pbnRzPSI0MjYsLTc0IDQyNiwtMTEyIDY3MiwtMTEyIDY3MiwtNzQgNDI2LC03NCIvPgo8cG9seWdvbiBmaWxsPSJub25lIiBzdHJva2U9IiMwMDAwMDAiIHBvaW50cz0iNDI2LC03NCA0MjYsLTExMiA2NzIsLTExMiA2NzIsLTc0IDQyNiwtNzQiLz4KPHRleHQgdGV4dC1hbmNob3I9InN0YXJ0IiB4PSI0MzYuNTk0NyIgeT0iLTg5LjQiIGZvbnQtZmFtaWx5PSJBcmlhbCIgZm9udC13ZWlnaHQ9ImJvbGQiIGZvbnQtc2l6ZT0iMTQuMDAiIGZpbGw9IiNmZmZmZmYiPiBQcm9kdWN0cy5Nb3N0UHJvZml0YWJsZVByb2R1Y3RzwqDCoDwvdGV4dD4KPC9nPgo8IS0tIFByb2R1Y3RzX1RvcFNlbGxpbmdQcm9maXRhYmxlUHJvZHVjdHMmIzQ1OyZndDtQcm9kdWN0c19Nb3N0UHJvZml0YWJsZVByb2R1Y3RzIC0tPgo8ZyBpZD0iZWRnZTciIGNsYXNzPSJlZGdlIj4KPHRpdGxlPlByb2R1Y3RzX1RvcFNlbGxpbmdQcm9maXRhYmxlUHJvZHVjdHMtJmd0O1Byb2R1Y3RzX01vc3RQcm9maXRhYmxlUHJvZHVjdHM8L3RpdGxlPgo8cGF0aCBmaWxsPSJub25lIiBzdHJva2U9IiMwMDAwMDAiIGQ9Ik0yODIuMjY3NiwtMTE3LjE4OUMzMjUuNTM4NCwtMTEzLjI2NDkgMzcyLjk4MDcsLTEwOC45NjI1IDQxNS43MDIxLC0xMDUuMDg4MyIvPgo8cG9seWdvbiBmaWxsPSIjMDAwMDAwIiBzdHJva2U9IiMwMDAwMDAiIHBvaW50cz0iNDE2LjIzODMsLTEwOC41NTQxIDQyNS44ODEzLC0xMDQuMTY1MiA0MTUuNjA2LC0xMDEuNTgyNyA0MTYuMjM4MywtMTA4LjU1NDEiLz4KPC9nPgo8IS0tIFByb2R1Y3RzX1RvcFNlbGxpbmdQcm9kdWN0cyAtLT4KPGcgaWQ9Im5vZGU4IiBjbGFzcz0ibm9kZSI+Cjx0aXRsZT5Qcm9kdWN0c19Ub3BTZWxsaW5nUHJvZHVjdHM8L3RpdGxlPgo8cG9seWdvbiBmaWxsPSIjMDAwMDhiIiBzdHJva2U9InRyYW5zcGFyZW50IiBwb2ludHM9IjQzOCwtMTQ4IDQzOCwtMTg2IDY2MSwtMTg2IDY2MSwtMTQ4IDQzOCwtMTQ4Ii8+Cjxwb2x5Z29uIGZpbGw9Im5vbmUiIHN0cm9rZT0iIzAwMDAwMCIgcG9pbnRzPSI0MzgsLTE0OCA0MzgsLTE4NiA2NjEsLTE4NiA2NjEsLTE0OCA0MzgsLTE0OCIvPgo8dGV4dCB0ZXh0LWFuY2hvcj0ic3RhcnQiIHg9IjQ0OC43NjE2IiB5PSItMTYzLjQiIGZvbnQtZmFtaWx5PSJBcmlhbCIgZm9udC13ZWlnaHQ9ImJvbGQiIGZvbnQtc2l6ZT0iMTQuMDAiIGZpbGw9IiNmZmZmZmYiPiBQcm9kdWN0cy5Ub3BTZWxsaW5nUHJvZHVjdHPCoMKgPC90ZXh0Pgo8L2c+CjwhLS0gUHJvZHVjdHNfVG9wU2VsbGluZ1Byb2ZpdGFibGVQcm9kdWN0cyYjNDU7Jmd0O1Byb2R1Y3RzX1RvcFNlbGxpbmdQcm9kdWN0cyAtLT4KPGcgaWQ9ImVkZ2U2IiBjbGFzcz0iZWRnZSI+Cjx0aXRsZT5Qcm9kdWN0c19Ub3BTZWxsaW5nUHJvZml0YWJsZVByb2R1Y3RzLSZndDtQcm9kdWN0c19Ub3BTZWxsaW5nUHJvZHVjdHM8L3RpdGxlPgo8cGF0aCBmaWxsPSJub25lIiBzdHJva2U9IiMwMDAwMDAiIGQ9Ik0yODIuMjY3NiwtMTQyLjgxMUMzMjkuNDY3LC0xNDcuMDkxNCAzODEuNjI5NiwtMTUxLjgyMTggNDI3LjIxNDgsLTE1NS45NTU4Ii8+Cjxwb2x5Z29uIGZpbGw9IiMwMDAwMDAiIHN0cm9rZT0iIzAwMDAwMCIgcG9pbnRzPSI0MjcuMTY0NSwtMTU5LjQ2NTUgNDM3LjQzOTgsLTE1Ni44ODMgNDI3Ljc5NjgsLTE1Mi40OTQxIDQyNy4xNjQ1LC0xNTkuNDY1NSIvPgo8L2c+CjwhLS0gUHJvZHVjdHNfTW9zdFByb2ZpdGFibGVQcm9kdWN0cyYjNDU7Jmd0O1Byb2R1Y3RzX1Byb2R1Y3RQcm9maXRhYmlsaXR5IC0tPgo8ZyBpZD0iZWRnZTgiIGNsYXNzPSJlZGdlIj4KPHRpdGxlPlByb2R1Y3RzX01vc3RQcm9maXRhYmxlUHJvZHVjdHMtJmd0O1Byb2R1Y3RzX1Byb2R1Y3RQcm9maXRhYmlsaXR5PC90aXRsZT4KPHBhdGggZmlsbD0ibm9uZSIgc3Ryb2tlPSIjMDAwMDAwIiBkPSJNNjcyLjIzMzMsLTkzQzcxNi45Mzg5LC05MyA3NjcuMzM1NCwtOTMgODExLjY5NjEsLTkzIi8+Cjxwb2x5Z29uIGZpbGw9IiMwMDAwMDAiIHN0cm9rZT0iIzAwMDAwMCIgcG9pbnRzPSI4MTEuOTQ3MSwtOTYuNTAwMSA4MjEuOTQ3MSwtOTMgODExLjk0NywtODkuNTAwMSA4MTEuOTQ3MSwtOTYuNTAwMSIvPgo8L2c+CjwhLS0gUHJvZHVjdHNfVG9wU2VsbGluZ1Byb2R1Y3RzJiM0NTsmZ3Q7UHJvZHVjdHNfUHJvZHVjdFBlcmZvcm1hbmNlIC0tPgo8ZyBpZD0iZWRnZTkiIGNsYXNzPSJlZGdlIj4KPHRpdGxlPlByb2R1Y3RzX1RvcFNlbGxpbmdQcm9kdWN0cy0mZ3Q7UHJvZHVjdHNfUHJvZHVjdFBlcmZvcm1hbmNlPC90aXRsZT4KPHBhdGggZmlsbD0ibm9uZSIgc3Ryb2tlPSIjMDAwMDAwIiBkPSJNNjYwLjUxNzEsLTE2N0M3MDUuOTYxMywtMTY3IDc1OC43MTE5LC0xNjcgODA1LjQ5NzIsLTE2NyIvPgo8cG9seWdvbiBmaWxsPSIjMDAwMDAwIiBzdHJva2U9IiMwMDAwMDAiIHBvaW50cz0iODA1LjY5NDIsLTE3MC41MDAxIDgxNS42OTQyLC0xNjcgODA1LjY5NDIsLTE2My41MDAxIDgwNS42OTQyLC0xNzAuNTAwMSIvPgo8L2c+CjwvZz4KPC9zdmc+"}}]);