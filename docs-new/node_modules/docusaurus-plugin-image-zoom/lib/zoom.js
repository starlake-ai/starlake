"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const tslib_1 = require("tslib");
const docusaurus_config_1 = tslib_1.__importDefault(require("@generated/docusaurus.config"));
const medium_zoom_1 = tslib_1.__importDefault(require("medium-zoom"));
const { themeConfig } = docusaurus_config_1.default;
function getBackgroundColor(zoom) {
    var _a, _b;
    const isDarkMode = document.querySelector('html[data-theme="dark"]');
    return isDarkMode
        ? ((_a = zoom.background) === null || _a === void 0 ? void 0 : _a.dark) || 'rgb(50, 50, 50)'
        : ((_b = zoom.background) === null || _b === void 0 ? void 0 : _b.light) || 'rgb(255, 255, 255)';
}
exports.default = (function () {
    if (typeof window === 'undefined') {
        return null;
    }
    let zoomObject;
    const { zoom } = themeConfig;
    const { selector = '.markdown img', config = {} } = zoom || {};
    if (!zoom) {
        return null;
    }
    config.background = getBackgroundColor(zoom);
    var observer = new MutationObserver(function () {
        if (!zoomObject) {
            return;
        }
        zoomObject.update({ background: getBackgroundColor(zoom) });
    });
    const htmlNode = document.querySelector('html');
    observer.observe(htmlNode, {
        attributes: true,
        attributeFilter: ['data-theme'],
    });
    setTimeout(() => {
        if (zoomObject) {
            zoomObject.detach();
        }
        zoomObject = (0, medium_zoom_1.default)(selector, config);
    }, 1000);
    return {
        onRouteUpdate() {
            setTimeout(() => {
                if (zoomObject) {
                    zoomObject.detach();
                }
                zoomObject = (0, medium_zoom_1.default)(selector, config);
            }, 1000);
        },
    };
})();
//# sourceMappingURL=zoom.js.map