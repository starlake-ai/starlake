"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.debugWarn = exports.debugInfo = exports.debugVerbose = void 0;
const tslib_1 = require("tslib");
const debug_1 = tslib_1.__importDefault(require("debug"));
const debug = (0, debug_1.default)("search-local");
exports.debugVerbose = debug.extend("verbose");
exports.debugInfo = debug.extend("info");
exports.debugWarn = debug.extend("warn");
