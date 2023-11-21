"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getIndexHash = void 0;
const tslib_1 = require("tslib");
const fs_1 = tslib_1.__importDefault(require("fs"));
const path_1 = tslib_1.__importDefault(require("path"));
const crypto_1 = tslib_1.__importDefault(require("crypto"));
const klaw_sync_1 = tslib_1.__importDefault(require("klaw-sync"));
const debug_1 = require("./debug");
function getIndexHash(config) {
    if (!config.hashed) {
        return null;
    }
    const files = [];
    const scanFiles = (flagField, dirField) => {
        if (config[flagField]) {
            for (const dir of config[dirField]) {
                if (!fs_1.default.existsSync(dir)) {
                    console.warn(`Warn: \`${dirField}\` doesn't exist: "${dir}".`);
                }
                else if (!fs_1.default.lstatSync(dir).isDirectory()) {
                    console.warn(`Warn: \`${dirField}\` is not a directory: "${dir}".`);
                }
                else {
                    files.push(...(0, klaw_sync_1.default)(dir, {
                        nodir: true,
                        filter: markdownFilter,
                        traverseAll: true,
                    }));
                }
            }
        }
    };
    scanFiles("indexDocs", "docsDir");
    scanFiles("indexBlog", "blogDir");
    if (files.length > 0) {
        const md5sum = crypto_1.default.createHash("md5");
        // The version of this plugin should be counted in hash,
        // since the index maybe changed between versions.
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const pluginVersion = require(path_1.default.resolve(__dirname, "../../../../package.json")).version;
        (0, debug_1.debugInfo)("using @easyops-cn/docusaurus-search-local v%s", pluginVersion);
        md5sum.update(pluginVersion, "utf8");
        for (const item of files) {
            md5sum.update(fs_1.default.readFileSync(item.path));
        }
        const indexHash = md5sum.digest("hex").substring(0, 8);
        (0, debug_1.debugInfo)("the index hash is %j", indexHash);
        return indexHash;
    }
    return null;
}
exports.getIndexHash = getIndexHash;
function markdownFilter(item) {
    return item.path.endsWith(".md") || item.path.endsWith(".mdx");
}
