"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const tslib_1 = require("tslib");
const path_1 = tslib_1.__importDefault(require("path"));
const validate_peer_dependencies_1 = tslib_1.__importDefault(require("validate-peer-dependencies"));
(0, validate_peer_dependencies_1.default)(__dirname);
module.exports = function () {
    return {
        name: 'docusaurus-plugin-image-zoom',
        getClientModules() {
            return [path_1.default.resolve(__dirname, './zoom')];
        },
    };
};
//# sourceMappingURL=index.js.map