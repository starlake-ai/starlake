"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
function rethrowUnlessCode(maybeError, ...codes) {
    if (maybeError !== null && typeof maybeError === 'object') {
        const code = maybeError.code;
        for (const allowed of codes) {
            if (code === allowed) {
                return;
            }
        }
    }
    throw maybeError;
}
exports.default = rethrowUnlessCode;
//# sourceMappingURL=rethrow-unless-code.js.map