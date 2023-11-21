"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.processDocInfos = void 0;
const tslib_1 = require("tslib");
const path_1 = tslib_1.__importDefault(require("path"));
const debug_1 = require("./debug");
function processDocInfos({ routesPaths, outDir, baseUrl, siteConfig, plugins }, { indexDocs, indexBlog, indexPages, docsRouteBasePath, blogRouteBasePath, ignoreFiles, }) {
    const emptySet = new Set();
    let versionData = new Map([[outDir, emptySet]]);
    if (plugins) {
        // Handle docs multi-instance.
        const docsPlugins = plugins.filter((item) => item.name === "docusaurus-plugin-content-docs");
        const loadedVersions = docsPlugins.flatMap((plugin) => plugin.content.loadedVersions);
        (0, debug_1.debugVerbose)("loadedVersions:", loadedVersions);
        if (loadedVersions.length > 0) {
            versionData = new Map();
            for (const loadedVersion of loadedVersions) {
                const route = loadedVersion.path.substr(baseUrl.length);
                let versionOutDir = outDir;
                // The last versions search-index should always be placed in the root since it is the one used from non-docs pages
                if (!loadedVersion.isLast) {
                    versionOutDir = path_1.default.join(outDir, ...route.split("/").filter((i) => i));
                }
                // Merge docs which share the same `versionOutDir`.
                let docs = versionData.get(versionOutDir);
                if (!docs) {
                    docs = new Set();
                    versionData.set(versionOutDir, docs);
                }
                for (const doc of loadedVersion.docs) {
                    docs.add(doc.permalink);
                }
            }
            (0, debug_1.debugVerbose)("versionData:", versionData);
        }
    }
    // Create a list of files to index per document version. This will always include all pages and blogs.
    const result = [];
    for (const [versionOutDir, docs] of versionData.entries()) {
        const versionPaths = routesPaths
            .map((url) => {
            // istanbul ignore next
            if (!url.startsWith(baseUrl)) {
                throw new Error(`The route must start with the baseUrl "${baseUrl}", but was "${url}". This is a bug, please report it.`);
            }
            const route = url.substr(baseUrl.length).replace(/\/$/, "");
            // Do not index homepage, error page and search page.
            if (((!docsRouteBasePath || docsRouteBasePath[0] !== "") &&
                route === "") ||
                route === "404.html" ||
                route === "search") {
                return;
            }
            // ignore files
            if (ignoreFiles === null || ignoreFiles === void 0 ? void 0 : ignoreFiles.some((reg) => {
                if (typeof reg === "string") {
                    return route === reg;
                }
                return route.match(reg);
            })) {
                return;
            }
            if (indexBlog &&
                blogRouteBasePath.some((basePath) => isSameOrSubRoute(route, basePath))) {
                if (blogRouteBasePath.some((basePath) => isSameRoute(route, basePath) ||
                    isSameOrSubRoute(route, path_1.default.posix.join(basePath, "tags")))) {
                    // Do not index list of blog posts and tags filter pages
                    return;
                }
                return { route, url, type: "blog" };
            }
            if (indexDocs &&
                docsRouteBasePath.some((basePath) => isSameOrSubRoute(route, basePath))) {
                if (docs.size === 0) {
                    return { route, url, type: 'docs' };
                }
                if (siteConfig.trailingSlash === true) {
                    // All routePaths have a trailing slash
                    // index.(md|*) permalink has trailing slash, other permalinks don't
                    if (docs.has(url) || docs.has(removeTrailingSlash(url))) {
                        return { route, url, type: 'docs' };
                    }
                }
                else if (siteConfig.trailingSlash === false) {
                    // All routePaths don't have a trailing slash
                    // index.(md|*) permalink has trailing slash, other permalinks don't
                    if (docs.has(addTrailingSlash(url)) || docs.has(url)) {
                        return { route, url, type: 'docs' };
                    }
                }
                else if (docs.has(url)) {
                    return { route, url, type: 'docs' };
                }
                return;
            }
            if (indexPages) {
                return { route, url, type: "page" };
            }
            return;
        })
            .filter(Boolean)
            .map(({ route, url, type }) => ({
            filePath: path_1.default.join(outDir, siteConfig.trailingSlash === false && route != ""
                ? `${route}.html`
                : `${route}/index.html`),
            url,
            type,
        }));
        if (versionPaths.length > 0) {
            result.push({ outDir: versionOutDir, paths: versionPaths });
        }
    }
    return result;
}
exports.processDocInfos = processDocInfos;
function isSameRoute(routeA, routeB) {
    return addTrailingSlash(routeA) === addTrailingSlash(routeB);
}
function isSameOrSubRoute(childRoute, parentRoute) {
    return (parentRoute === "" ||
        addTrailingSlash(childRoute).startsWith(addTrailingSlash(parentRoute)));
}
// The input route must not end with a slash.
function addTrailingSlash(route) {
    return `${route}/`;
}
function removeTrailingSlash(route) {
    return route.replace(/\/$/, '');
}
