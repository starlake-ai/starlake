/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
    title: "Quickly Build Optimized Big & Fast Data Pipelines",
    tagline: 'So your data just keep moving',
    url: 'https://starlake-ai.github.io',
    baseUrl: process.env.BASE_URL || '/',
    onBrokenLinks: 'throw',
    onBrokenMarkdownLinks: 'warn',
    favicon: 'img/favicon.ico',
    organizationName: 'starlake-ai', // Usually your GitHub org/user name.
    projectName: 'starlake', // Usually your repo name.
    themeConfig: {
        hideableSidebar: false,
        prism: {
            additionalLanguages: ['java', 'scala', 'sql', 'powershell'],
            theme: require('prism-react-renderer/themes/github'),
            darkTheme: require('prism-react-renderer/themes/dracula'),
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
                    label: 'Documentation',
                    position: 'left',
                    items: [
                        {
                            label: 'User Guide',
                            to: '/docs/userguide/load'
                        },
                        {
                            to: '/docs/reference/configuration',
                            label: 'Reference'
                        },
                        {
                            to: '/docs/cli/import',
                            label: 'CLI'
                        },
                    ]
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
                            label: 'Discord',
                            href: 'https://discord.com/channels/833336395430625310/908709208025858079',
                        },
                        {
                            label: 'Stack Overflow',
                            href: 'https://stackoverflow.com/questions/tagged/starlake',
                        },
                    ]
                },
                {
                    href: 'https://search.maven.org/search?q=ai.starlake',
                    position: 'right',
                    className: 'header-download-link header-icon-link',
                    'aria-label': 'Download',
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
                            label: 'Discord',
                            href: 'https://discord.com/channels/833336395430625310/908709208025858079',
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
            copyright: `Built with Docusaurus.`,
        },
    },
    presets: [
        [
            '@docusaurus/preset-classic',
            {
                googleAnalytics: {
                    trackingID: 'UA-207943293-1',
                    // Optional fields.
                    anonymizeIP: true // Should IPs be anonymized?
                },
                gtag: {
                    trackingID: 'G-FYS72XYD48',
                    anonymizeIP: true,
                },
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
            },
        ],
    ],
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
