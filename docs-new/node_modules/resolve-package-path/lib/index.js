'use strict';
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
const path_1 = __importDefault(require("path"));
const resolve_package_path_1 = __importDefault(require("./resolve-package-path"));
const rethrow_unless_code_1 = __importDefault(require("./rethrow-unless-code"));
const ALLOWED_ERROR_CODES = [
    // resolve package error codes
    'MODULE_NOT_FOUND',
    // Yarn PnP Error Codes
    'UNDECLARED_DEPENDENCY',
    'MISSING_PEER_DEPENDENCY',
    'MISSING_DEPENDENCY'
];
const CacheGroup = require("./cache-group");
const Cache = require("./cache");
const getRealFilePath = resolve_package_path_1.default._getRealFilePath;
const getRealDirectoryPath = resolve_package_path_1.default._getRealDirectoryPath;
const __findUpPackagePath = resolve_package_path_1.default._findUpPackagePath;
let CACHE = new CacheGroup();
let FIND_UP_CACHE = new Cache();
let pnp;
try {
    // eslint-disable-next-line node/no-missing-require
    pnp = require('pnpapi');
}
catch (error) {
    // not in Yarn PnP; not a problem
}
/**
 * Search each directory in the absolute path `baseDir`, from leaf to root, for
 * a `package.json`, and return the first match, or `null` if no `package.json`
 * was found.
 *
 * @public
 * @param {string} baseDir - an absolute path in which to search for a `package.json`
 * @param {CacheGroup|boolean} [_cache] (optional)
 *  * if true: will choose the default global cache
 *  * if false: will not cache
 *  * if undefined or omitted, will choose the default global cache
 *  * otherwise we assume the argument is an external cache of the form provided by resolve-package-path/lib/cache-group.js
 *
 * @return {string|null} a full path to the resolved package.json if found or null if not
 */
function _findUpPackagePath(baseDir, _cache) {
    let cache;
    if (_cache === undefined || _cache === null || _cache === true) {
        // if no cache specified, or if cache is true then use the global cache
        cache = FIND_UP_CACHE;
    }
    else if (_cache === false) {
        // if cache is explicity false, create a throw-away cache;
        cache = new Cache();
    }
    else {
        // otherwise, assume the user has provided an alternative cache for the following form:
        // provided by resolve-package-path/lib/cache-group.js
        cache = _cache;
    }
    let absoluteStart = path_1.default.resolve(baseDir);
    return __findUpPackagePath(cache, absoluteStart);
}
function resolvePackagePath(target, baseDir, _cache) {
    let cache;
    if (_cache === undefined || _cache === null || _cache === true) {
        // if no cache specified, or if cache is true then use the global cache
        cache = CACHE;
    }
    else if (_cache === false) {
        // if cache is explicity false, create a throw-away cache;
        cache = new CacheGroup();
    }
    else {
        // otherwise, assume the user has provided an alternative cache for the following form:
        // provided by resolve-package-path/lib/cache-group.js
        cache = _cache;
    }
    if (baseDir.charAt(baseDir.length - 1) !== path_1.default.sep) {
        baseDir = `${baseDir}${path_1.default.sep}`;
    }
    const key = target + '\x00' + baseDir;
    let pkgPath;
    if (cache.PATH.has(key)) {
        pkgPath = cache.PATH.get(key);
    }
    else {
        try {
            // the custom `pnp` code here can be removed when yarn 1.13 is the
            // current release. This is due to Yarn 1.13 and resolve interoperating
            // together seamlessly.
            pkgPath = pnp
                ? pnp.resolveToUnqualified(target + '/package.json', baseDir)
                : (0, resolve_package_path_1.default)(cache, target, baseDir);
        }
        catch (e) {
            (0, rethrow_unless_code_1.default)(e, ...ALLOWED_ERROR_CODES);
            pkgPath = null;
        }
        cache.PATH.set(key, pkgPath);
    }
    return pkgPath;
}
resolvePackagePath._resetCache = function () {
    CACHE = new CacheGroup();
    FIND_UP_CACHE = new Cache();
};
// eslint-disable-next-line no-redeclare
(function (resolvePackagePath) {
    resolvePackagePath._FIND_UP_CACHE = FIND_UP_CACHE;
    resolvePackagePath.findUpPackagePath = _findUpPackagePath;
})(resolvePackagePath || (resolvePackagePath = {}));
Object.defineProperty(resolvePackagePath, '_CACHE', {
    get: function () {
        return CACHE;
    },
});
Object.defineProperty(resolvePackagePath, '_FIND_UP_CACHE', {
    get: function () {
        return FIND_UP_CACHE;
    },
});
resolvePackagePath.getRealFilePath = function (filePath) {
    return getRealFilePath(CACHE.REAL_FILE_PATH, filePath);
};
resolvePackagePath.getRealDirectoryPath = function (directoryPath) {
    return getRealDirectoryPath(CACHE.REAL_DIRECTORY_PATH, directoryPath);
};
module.exports = resolvePackagePath;
//# sourceMappingURL=index.js.map