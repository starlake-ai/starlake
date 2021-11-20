/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
    title: "Quickly Build Optimized Big & Fast Data Pipelines",
    tagline: 'So your data just keep moving',
    url: 'https://starlake-ai.github.io',
    baseUrl: '/starlake/',
    onBrokenLinks: 'throw',
    onBrokenMarkdownLinks: 'warn',
    favicon: 'img/favicon.ico',
    organizationName: 'starlake-ai', // Usually your GitHub org/user name.
    projectName: 'starlake', // Usually your repo name.
    themeConfig: {
        googleAnalytics: {
            trackingID: 'G-S16TFWHZYT',
            // Optional fields.
            anonymizeIP: true // Should IPs be anonymized?
        },

        navbar: {
            title: 'Starlake',
            logo: {
                alt: 'Starlake',
                src: 'img/starlake.png',
            },
            items: [
                {
                    type: 'doc',
                    docId: 'intro',
                    position: 'left',
                    label: 'Getting Started',
                },
                {to: '/blog', label: 'Blog', position: 'left'},
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
                        'https://github.com/starlake-ai/starlake/edit/master/docs/blog/',
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
