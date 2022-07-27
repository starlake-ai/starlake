import React from 'react';
import ComponentCreator from '@docusaurus/ComponentCreator';

export default [
  {
    path: '/__docusaurus/debug',
    component: ComponentCreator('/__docusaurus/debug', 'b25'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/config',
    component: ComponentCreator('/__docusaurus/debug/config', 'ed0'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/content',
    component: ComponentCreator('/__docusaurus/debug/content', 'f9a'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/globalData',
    component: ComponentCreator('/__docusaurus/debug/globalData', '172'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/metadata',
    component: ComponentCreator('/__docusaurus/debug/metadata', '9db'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/registry',
    component: ComponentCreator('/__docusaurus/debug/registry', 'c55'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/routes',
    component: ComponentCreator('/__docusaurus/debug/routes', '19b'),
    exact: true
  },
  {
    path: '/blog',
    component: ComponentCreator('/blog', '51b'),
    exact: true
  },
  {
    path: '/blog/archive',
    component: ComponentCreator('/blog/archive', 'cd4'),
    exact: true
  },
  {
    path: '/blog/bonjour',
    component: ComponentCreator('/blog/bonjour', '599'),
    exact: true
  },
  {
    path: '/blog/rls-cls-big-query',
    component: ComponentCreator('/blog/rls-cls-big-query', '86d'),
    exact: true
  },
  {
    path: '/blog/spark-big-query-partitioning',
    component: ComponentCreator('/blog/spark-big-query-partitioning', '9e4'),
    exact: true
  },
  {
    path: '/blog/tags',
    component: ComponentCreator('/blog/tags', '6f1'),
    exact: true
  },
  {
    path: '/blog/tags/big-query',
    component: ComponentCreator('/blog/tags/big-query', 'c98'),
    exact: true
  },
  {
    path: '/blog/tags/bonjour',
    component: ComponentCreator('/blog/tags/bonjour', 'a23'),
    exact: true
  },
  {
    path: '/blog/tags/dataproc',
    component: ComponentCreator('/blog/tags/dataproc', '6c9'),
    exact: true
  },
  {
    path: '/blog/tags/etl',
    component: ComponentCreator('/blog/tags/etl', 'd25'),
    exact: true
  },
  {
    path: '/blog/tags/google-cloud',
    component: ComponentCreator('/blog/tags/google-cloud', 'ceb'),
    exact: true
  },
  {
    path: '/blog/tags/spark',
    component: ComponentCreator('/blog/tags/spark', '510'),
    exact: true
  },
  {
    path: '/blog/tags/starlake',
    component: ComponentCreator('/blog/tags/starlake', '69d'),
    exact: true
  },
  {
    path: '/markdown-page',
    component: ComponentCreator('/markdown-page', '6d8'),
    exact: true
  },
  {
    path: '/search',
    component: ComponentCreator('/search', '16a'),
    exact: true
  },
  {
    path: '/docs',
    component: ComponentCreator('/docs', 'f86'),
    routes: [
      {
        path: '/docs/cli/bqload',
        component: ComponentCreator('/docs/cli/bqload', 'dad'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/cnxload',
        component: ComponentCreator('/docs/cli/cnxload', '838'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/ddl2yml',
        component: ComponentCreator('/docs/cli/ddl2yml', '045'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/esload',
        component: ComponentCreator('/docs/cli/esload', 'a8e'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/extract',
        component: ComponentCreator('/docs/cli/extract', '0cd'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/import',
        component: ComponentCreator('/docs/cli/import', '00c'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/infer-schema',
        component: ComponentCreator('/docs/cli/infer-schema', '638'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/kafkaload',
        component: ComponentCreator('/docs/cli/kafkaload', '3fc'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/load',
        component: ComponentCreator('/docs/cli/load', 'c0b'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/metrics',
        component: ComponentCreator('/docs/cli/metrics', '217'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/parquet2csv',
        component: ComponentCreator('/docs/cli/parquet2csv', '006'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/transform',
        component: ComponentCreator('/docs/cli/transform', '960'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/watch',
        component: ComponentCreator('/docs/cli/watch', 'fdb'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/xls2yml',
        component: ComponentCreator('/docs/cli/xls2yml', '513'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/yml2gv',
        component: ComponentCreator('/docs/cli/yml2gv', '977'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/cli/yml2xls',
        component: ComponentCreator('/docs/cli/yml2xls', '096'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/devguide/contribute',
        component: ComponentCreator('/docs/devguide/contribute', '110'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/intro',
        component: ComponentCreator('/docs/intro', '84c'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/platform/aws',
        component: ComponentCreator('/docs/platform/aws', 'bcc'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/platform/azure',
        component: ComponentCreator('/docs/platform/azure', 'ee0'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/platform/databricks',
        component: ComponentCreator('/docs/platform/databricks', 'ee7'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/platform/file',
        component: ComponentCreator('/docs/platform/file', '831'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/platform/gcp',
        component: ComponentCreator('/docs/platform/gcp', '9ff'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/platform/hadoop',
        component: ComponentCreator('/docs/platform/hadoop', '339'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/reference/configuration',
        component: ComponentCreator('/docs/reference/configuration', 'a33'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/reference/environment',
        component: ComponentCreator('/docs/reference/environment', 'cc8'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/reference/examples',
        component: ComponentCreator('/docs/reference/examples', 'd86'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/reference/extract',
        component: ComponentCreator('/docs/reference/extract', 'd30'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/reference/load',
        component: ComponentCreator('/docs/reference/load', '21a'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/reference/scheduling',
        component: ComponentCreator('/docs/reference/scheduling', '2b6'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/reference/transform',
        component: ComponentCreator('/docs/reference/transform', '76b'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/userguide/load',
        component: ComponentCreator('/docs/userguide/load', 'fd8'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/userguide/quickstart',
        component: ComponentCreator('/docs/userguide/quickstart', '860'),
        exact: true,
        sidebar: "starlakeSidebar"
      },
      {
        path: '/docs/userguide/transform',
        component: ComponentCreator('/docs/userguide/transform', 'eaf'),
        exact: true,
        sidebar: "starlakeSidebar"
      }
    ]
  },
  {
    path: '/',
    component: ComponentCreator('/', '3b6'),
    exact: true
  },
  {
    path: '*',
    component: ComponentCreator('*'),
  },
];
