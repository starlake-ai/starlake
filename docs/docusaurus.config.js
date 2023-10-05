// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
    title: "Supercharge your data pipelines",
    tagline: 'So your data just keep moving',
    favicon: 'img/favicon.ico',

    url: 'https://starlake-ai.github.io',
    baseUrl: process.env.BASE_URL || '/',

    organizationName: 'starlake-ai', // Usually your GitHub org/user name.
    projectName: 'starlake', // Usually your repo name.

    onBrokenLinks: 'throw',
    onBrokenMarkdownLinks: 'warn',

    // Even if you don't use internalization, you can use this field to set useful
    // metadata like html lang. For example, if your site is Chinese, you may want
    // to replace "en" with "zh-Hans".
    i18n: {
        defaultLocale: 'en',
        locales: ['en'],
    },
    presets: [
        [
            'classic',
            ({
                docs: {
                    sidebarPath: require.resolve('./sidebars.js'),
                    // Please change this to your repo.
                    editUrl:
                        'https://github.com/starlake-ai/starlake/edit/master/docs/',
                },
                blog: {
                    showReadingTime: true,
                    // Please change this to your repo.
                    editUrl:
                        'https://github.com/starlake-ai/starlake/edit/master/docs/',
                },
                theme: {
                    customCss: require.resolve('./src/css/custom.css'),
                },
                googleAnalytics: {
                    trackingID: 'UA-207943293-1',
                    // Optional fields.
                    anonymizeIP: true // Should IPs be anonymized?
                },
                gtag: {
                    trackingID: 'G-FYS72XYD48',
                    anonymizeIP: true,
                },
            }),
        ],
    ],
    themeConfig: ({

        docs: {
            sidebar: {
                autoCollapseCategories: true,
                hideable: false
            }
        },
        prism: {
            additionalLanguages: ['java', 'scala', 'sql', 'powershell', 'python'],
            theme: lightCodeTheme,
            darkTheme: darkCodeTheme
        },
        navbar: {
            title: 'Starlake',
            logo: {
                alt: 'Starlake',
                src: 'img/shooting-star.png',
                srcDark: 'img/shooting-star-white.png',
            },
            items: [
                {
                    type: 'docSidebar',
                    sidebarId: 'starlakeSidebar',
                    label: 'Documentation',
                    position: 'left'
                },
                {to: '/blog', label: 'Blog', position: 'left'},
                {
                    label: 'Community',
                    position: 'left',
                    items: [
                        {
                            label: 'GitHub',
                            href: 'https://github.com/starlake-ai/starlake',
                        },
                        {
                            label: 'Slack',
                            href: 'https://starlakeai.slack.com',
                        },
                        {
                            label: 'Stack Overflow',
                            href: 'https://stackoverflow.com/questions/tagged/starlake',
                        },
                    ]
                },
                {
                    type: 'docsVersionDropdown',
                    position: 'right',
                },
                {
                    href: 'https://search.maven.org/search?q=ai.starlake',
                    position: 'right',
                    className: 'header-download-link header-icon-link',
                    'aria-label': 'Download',
                },
                {
                    href: 'https://starlakeai.slack.com',
                    position: 'right',
                    className: 'header-slack-link header-icon-link',
                    'aria-label': 'Community',
                },
                {
                    href: 'https://github.com/starlake-ai/starlake',
                    position: 'right',
                    className: 'header-github-link header-icon-link',
                    'aria-label': 'GitHub repository',
                },

            ],
        },
        footer: {
            style: 'dark',
            links: [
                {
                    title: 'Docs',
                    items: [
                        {
                            label: 'Tutorial',
                            to: '/docs/intro',
                        },
                    ],
                },
                {
                    title: 'Community',
                    items: [
                        {
                            label: 'Stack Overflow',
                            href: 'https://stackoverflow.com/questions/tagged/starlake',
                        },
                        {
                            label: 'Slack',
                            href: 'https://starlakeai.slack.com',
                        },
                        {
                            label: 'Twitter',
                            href: 'https://twitter.com/starlake-ai',
                        },
                    ],
                },
                {
                    title: 'More',
                    items: [
                        {
                            label: 'Blog',
                            to: '/blog',
                        },
{
                            label: 'GitHub',
                            href: 'https://github.com/starlake-ai/starlake',
                        },
                    ],
                },
            ],
            copyright: `By the way, Starlake is serverless.`,
        },
    }),

    plugins: [
        // ... Your other plugins.
        [
            require.resolve("@easyops-cn/docusaurus-search-local"),
            {
                // ... Your options.
                // `hashed` is recommended as long-term-cache of index file is possible.
                hashed: true,
                // For Docs using Chinese, The `language` is recommended to set to:
                // ```
                // language: ["en", "zh"],
                // ```
                // When applying `zh` in language, please install `nodejieba` in your project.
            },
            
        ],
    ]
};

module.exports = config;
