// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
    title: "Supercharge your data pipelines",
    tagline: 'So your data just keep moving',
    favicon: 'img/favicon_starlake.ico',

    // Set the production url of your site here
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
            additionalLanguages: ['java', 'scala', 'sql', 'powershell', 'python']
        },
        // Replace with your project's social card
        colorMode: {
            defaultMode: 'dark',
            disableSwitch: true,
            respectPrefersColorScheme: false,
        },
        navbar: {
            title: 'Starlake',
            logo: {
                alt: 'Starlake',
                src: 'img/starlake_icon.svg',
                srcDark: 'img/starlake_icon.svg',
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
                    type: 'docsVersionDropdown',
                    position: 'right',
                },
                /*
                {
                    href: 'https://search.maven.org/search?q=ai.starlake',
                    position: 'right',
                    className: 'header-download-link header-icon-link',
                    'aria-label': 'Download',
                },
                */
                {
                    href: 'https://github.com/starlake-ai/starlake',
                    position: 'right',
                    className: 'header-github-link header-icon-link',
                    'aria-label': 'GitHub repository',
                },
                {
                    href: 'https://starlakeai.slack.com',
                    position: 'right',
                    className: 'header-slack-link header-icon-link',
                    'aria-label': 'Community',
                },

            ],
        },
        footer: {
            style: 'dark',
            links: [
                {
                    items: [
                        {
                            label: 'Documentation',
                            to: '/docs/intro',
                        },
                        {
                            label: 'Blog',
                            href: '/blog',
                        },
                    ],
                },

                {
                    items: [
                        {
                            html: `
<ul class="footer_right"><li><a href="https://github.com/starlake-ai/starlake" target="_blank" rel="noopener noreferrer" class="navbar__item navbar__link header-github-link header-icon-link" aria-label="GitHub repository"></a></li><li><a href="https://starlakeai.slack.com" target="_blank" rel="noopener noreferrer" class="navbar__item navbar__link header-slack-link header-icon-link" aria-label="Community"></a></li></ul>
`,
                        },
                    ]
                },
            ],
        },
        zoom: {
            selector: '.markdown :not(em) > img, .split_section .img-fluid',
            config: {
                // options you can specify via https://github.com/francoischalifour/medium-zoom#usage
                background: {
                    light: 'rgba(2, 0, 19, 0.5)',
                    dark: 'rgba(2, 0, 19, 0.5)'
                }
            }
        }
    }),

    plugins: [
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
        [
            require.resolve("docusaurus-plugin-image-zoom"),
            {
                hashed: true,
            },
        ],
    ],
    markdown: {
        mermaid: true,
    },
    themes: ['@docusaurus/theme-mermaid']
};

module.exports = config;
