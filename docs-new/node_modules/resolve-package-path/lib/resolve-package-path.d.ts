import Cache = require('./cache');
import CacheGroup = require('./cache-group');
declare function resolvePackagePath(caches: CacheGroup, name?: string, dir?: string): string | null;
declare namespace resolvePackagePath {
    var _findPackagePath: (realFilePathCache: Cache, name: string, dir: string) => string | null;
    var _findUpPackagePath: (findUpCache: Cache, initialSearchDir: string) => string | null;
    var _getRealFilePath: (realFilePathCache: Cache, filePath: string) => string | null;
    var _getRealDirectoryPath: (realDirectoryPathCache: Cache, directoryPath: string) => string | null;
}
export = resolvePackagePath;
