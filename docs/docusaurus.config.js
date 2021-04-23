/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
    title: 'Comet Data Pipeline',
    tagline: 'Comet Data Pipeline is a Spark Based On Premise and Cloud Ingestion & Transformation Framework for Batch & Stream Processing Systems',
    url: 'https://ebiznext.github.io',
    baseUrl: '/comet-data-pipeline/',
    onBrokenLinks: 'throw',
    onBrokenMarkdownLinks: 'warn',
    favicon: 'img/favicon.ico',
    organizationName: 'ebiznext', // Usually your GitHub org/user name.
    projectName: 'comet-data-pipeline', // Usually your repo name.
    themeConfig: {
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
                    href: 'https://github.com/ebiznext/comet-data-pipeline',
                    label: 'GitHub',
                    position: 'right',
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
                            label: 'Discord',
                            href: 'https://discordapp.com/invite/comet-data-pipeline',
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
                docs: {
                    sidebarPath: require.resolve('./sidebars.js'),
                    // Please change this to your repo.
                    editUrl:
                        'https://github.com/facebook/docusaurus/edit/master/website/',
                },
                blog: {
                    showReadingTime: true,
                    // Please change this to your repo.
                    editUrl:
                        'https://github.com/facebook/docusaurus/edit/master/website/blog/',
                },
                theme: {
                    customCss: require.resolve('./src/css/custom.css'),
                },
            },
        ],
    ],
};
