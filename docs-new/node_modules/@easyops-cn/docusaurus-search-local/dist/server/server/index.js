"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.validateOptions = void 0;
const tslib_1 = require("tslib");
const path_1 = tslib_1.__importDefault(require("path"));
const fs_extra_1 = tslib_1.__importDefault(require("fs-extra"));
const utils_1 = require("@docusaurus/utils");
const theme_translations_1 = require("@docusaurus/theme-translations");
const processPluginOptions_1 = require("./utils/processPluginOptions");
const postBuildFactory_1 = require("./utils/postBuildFactory");
const generate_1 = require("./utils/generate");
const PLUGIN_NAME = "@easyops-cn/docusaurus-search-local";
function DocusaurusSearchLocalPlugin(context, options) {
    const config = (0, processPluginOptions_1.processPluginOptions)(options, context);
    const dir = path_1.default.join(context.generatedFilesDir, PLUGIN_NAME, "default");
    fs_extra_1.default.ensureDirSync(dir);
    const searchIndexFilename = (0, generate_1.generate)(config, dir);
    const themePath = path_1.default.resolve(__dirname, "../../client/client/theme");
    const pagePath = path_1.default.join(themePath, "SearchPage/index.js");
    return {
        name: PLUGIN_NAME,
        getThemePath() {
            return themePath;
        },
        postBuild: (0, postBuildFactory_1.postBuildFactory)(config, searchIndexFilename),
        getPathsToWatch() {
            return [pagePath];
        },
        getDefaultCodeTranslationMessages() {
            return tslib_1.__awaiter(this, void 0, void 0, function* () {
                const dirPath = path_1.default.join(__dirname, "../../locales");
                const localesToTry = (0, theme_translations_1.codeTranslationLocalesToTry)(context.i18n.currentLocale);
                // Return the content of the first file that match
                // fr_FR.json => fr.json => nothing
                for (const locale of localesToTry) {
                    const filePath = path_1.default.resolve(dirPath, `${locale}.json`);
                    if (yield fs_extra_1.default.pathExists(filePath)) {
                        return fs_extra_1.default.readJSON(filePath);
                    }
                }
                return {};
            });
        },
        contentLoaded({ actions: { addRoute } }) {
            return tslib_1.__awaiter(this, void 0, void 0, function* () {
                addRoute({
                    path: (0, utils_1.normalizeUrl)([context.baseUrl, "search"]),
                    component: "@theme/SearchPage",
                    exact: true,
                });
            });
        },
    };
}
exports.default = DocusaurusSearchLocalPlugin;
var validateOptions_1 = require("./utils/validateOptions");
Object.defineProperty(exports, "validateOptions", { enumerable: true, get: function () { return validateOptions_1.validateOptions; } });
