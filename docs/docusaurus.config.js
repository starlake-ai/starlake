/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
    title: "Quickly Build Optimized Big & Fast Data Pipelines",
    tagline: 'So your data just keep moving',
    url: 'https://ebiznext.github.io',
    baseUrl: '/comet-data-pipeline/',
    onBrokenLinks: 'throw',
    onBrokenMarkdownLinks: 'warn',
    favicon: 'img/favicon.ico',
    organizationName: 'ebiznext', // Usually your GitHub org/user name.
    projectName: 'comet-data-pipeline', // Usually your repo name.
    themeConfig: {
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
            title: 'Comet Data Pipeline',
            logo: {
                alt: 'Comet Data Pipeline',
                src: 'img/comet.png',
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
                    href: 'https://github.com/ebiznext/comet-data-pipeline/releases/latest',
                    position: 'right',
                    className: 'header-download-link header-icon-link',
                    'aria-label': 'Download',
                },
                {
                    href: 'https://github.com/ebiznext/comet-data-pipeline',
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
                            href: 'https://stackoverflow.com/questions/tagged/comet-data-pipeline',
                        },
                        {
                            label: 'Gitter',
                            href: 'https://gitter.im/comet-data-pipeline/community',
                        },
                        {
                            label: 'Twitter',
                            href: 'https://twitter.com/comet-data-pipeline',
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
                            href: 'https://github.com/ebiznext/comet-data-pipeline',
                        },
                    ],
                },
            ],
            copyright: `Copyright © ${new Date().getFullYear()} Comet Data Pipeline, Inc. Built with Docusaurus.`,
        },
    },
    presets: [
        [
            '@docusaurus/preset-classic',
            {
                googleAnalytics: {
                    trackingId: 'G-S0KK4KS0JD',
                    // Optional fields.
                    anonymizeIP: true // Should IPs be anonymized?
                },
                docs: {
                    sidebarPath: require.resolve('./sidebars.js'),
                    // Please change this to your repo.
                    editUrl:
                        'https://github.com/ebiznext/comet-data-pipeline/edit/master/docs/',
                },
                blog: {
                    showReadingTime: true,
                    // Please change this to your repo.
                    editUrl:
                        'https://github.com/ebiznext/comet-data-pipeline/edit/master/docs/blog/',
                },
                theme: {
                    customCss: require.resolve('./src/css/custom.css'),
                },
            },
        ],
    ],
};
