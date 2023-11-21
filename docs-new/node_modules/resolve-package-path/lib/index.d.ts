import CacheGroup = require('./cache-group');
import Cache = require('./cache');
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
declare function _findUpPackagePath(baseDir: string, _cache?: Cache | boolean): string | null;
export = resolvePackagePath;
declare function resolvePackagePath(target: string, baseDir: string, _cache?: CacheGroup | boolean): string | null;
declare namespace resolvePackagePath {
    var _resetCache: () => void;
    var getRealFilePath: (filePath: string) => string | null;
    var getRealDirectoryPath: (directoryPath: string) => string | null;
}
declare module resolvePackagePath {
    let _CACHE: CacheGroup;
    let _FIND_UP_CACHE: Cache;
    let findUpPackagePath: typeof _findUpPackagePath;
}
