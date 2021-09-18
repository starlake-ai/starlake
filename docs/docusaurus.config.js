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
        algolia: {
            apiKey: 'ac0b111da6354964b01b7e634e6d73fe',
            indexName: 'comet-website',

            // Optional: see doc section below
            contextualSearch: true,


            // Optional: see doc section below
            appId: 'IEP5P8HN5L',

            // Optional: Algolia search parameters
            searchParameters: {},

            //... other Algolia params
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
                    href: 'https://github.com/starlake-ai/starlake/releases/latest',
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
                            label: 'Gitter',
                            href: 'https://gitter.im/starlake/community',
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
};
