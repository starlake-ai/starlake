'use strict';

const resolvePackagePath = require('resolve-package-path');
const semver = require('semver');
const path = require('path');

// avoid checking multiple times from the same location
const HasPeerDepsInstalled = new Map();

const NullCache = new (class NullCache {
  get() {}
  set() {}
  has() {
    return false;
  }
})();

function throwUsefulError(result) {
  let { missingPeerDependencies, incompatibleRanges } = result;

  let missingPeerDependenciesMessage = (missingPeerDependencies || []).reduce(
    (message, metadata) => {
      return `${message}\n\t* ${metadata.name}: \`${metadata.specifiedPeerDependencyRange}\`; it was not installed`;
    },
    ''
  );

  let incompatiblePeerDependenciesMessage = (incompatibleRanges || []).reduce(
    (message, metadata) => {
      return `${message}\n\t* ${metadata.name}: \`${metadata.specifiedPeerDependencyRange}\`; it was resolved to \`${metadata.version}\``;
    },
    ''
  );

  throw new Error(
    `${result.pkg.name} has the following unmet peerDependencies:\n${missingPeerDependenciesMessage}${incompatiblePeerDependenciesMessage}`
  );
}

function resolvePackageVersion(
  packageName,
  resolvePeerDependenciesFrom,
  cache
) {
  let assumedVersion = AssumptionMap.get(packageName);
  if (assumedVersion !== undefined) {
    return assumedVersion;
  }

  let peerDepPackagePath = resolvePackagePath(
    packageName,
    resolvePeerDependenciesFrom,
    cache === NullCache ? false : undefined
  );

  if (peerDepPackagePath === null) {
    return null;
  }

  return require(peerDepPackagePath).version;
}

module.exports = function validatePeerDependencies(parentRoot, options = {}) {
  if (
    process.env.VALIDATE_PEER_DEPENDENCIES &&
    process.env.VALIDATE_PEER_DEPENDENCIES.toLowerCase() === 'false'
  ) {
    // do not validate
    return;
  }

  let { cache, handleFailure, resolvePeerDependenciesFrom } = options;

  if (cache === false) {
    cache = NullCache;
  } else if (cache === undefined || cache === true) {
    cache = HasPeerDepsInstalled;
  }

  if (typeof handleFailure !== 'function') {
    handleFailure = throwUsefulError;
  }

  let cacheKey = parentRoot;

  if (resolvePeerDependenciesFrom === undefined) {
    resolvePeerDependenciesFrom = parentRoot;
  } else {
    cacheKey += `\0${resolvePeerDependenciesFrom}`;
  }

  if (cache.has(cacheKey)) {
    let result = cache.get(cacheKey);
    if (result !== true) {
      handleFailure(result);
    }
    return;
  }

  let packagePath = resolvePackagePath.findUpPackagePath(
    parentRoot,
    cache === NullCache ? false : undefined
  );

  if (packagePath === null) {
    throw new Error(
      `validate-peer-dependencies could not find a package.json when resolving upwards from:\n\t${parentRoot}.`
    );
  }

  let ignoredPeerDependencies = process.env.IGNORE_PEER_DEPENDENCIES
    ? new Set(process.env.IGNORE_PEER_DEPENDENCIES.split(','))
    : new Set();

  let pkg = require(packagePath);
  let { dependencies, peerDependencies, peerDependenciesMeta } = pkg;
  let hasDependencies = Boolean(dependencies);
  let hasPeerDependenciesMeta = Boolean(peerDependenciesMeta);

  // lazily created as needed
  let missingPeerDependencies = null;
  let incompatibleRanges = null;
  let invalidPackageConfiguration = null;

  for (let packageName in peerDependencies) {
    if (hasDependencies && packageName in dependencies) {
      if (invalidPackageConfiguration === null) {
        invalidPackageConfiguration = [];
      }

      invalidPackageConfiguration.push({
        name: packageName,
        reason: 'included both as dependency and as a peer dependency',
      });
    }

    if (ignoredPeerDependencies.has(packageName)) {
      continue;
    }

    //   foo-package: >= 1.9.0 < 2.0.0
    //   foo-package: >= 1.9.0
    //   foo-package: ^1.9.0
    let specifiedPeerDependencyRange = peerDependencies[packageName];

    let foundPackageVersion = resolvePackageVersion(
      packageName,
      resolvePeerDependenciesFrom,
      cache
    );

    if (foundPackageVersion === null) {
      if (
        hasPeerDependenciesMeta &&
        packageName in peerDependenciesMeta &&
        peerDependenciesMeta[packageName].optional
      ) {
        continue;
      }

      if (missingPeerDependencies === null) {
        missingPeerDependencies = [];
      }

      missingPeerDependencies.push({
        name: packageName,
        specifiedPeerDependencyRange,
      });

      continue;
    }

    if (
      !semver.satisfies(foundPackageVersion, specifiedPeerDependencyRange, {
        includePrerelease: true,
      })
    ) {
      if (incompatibleRanges === null) {
        incompatibleRanges = [];
      }

      incompatibleRanges.push({
        name: packageName,
        version: foundPackageVersion,
        specifiedPeerDependencyRange,
      });

      continue;
    }
  }

  if (invalidPackageConfiguration !== null) {
    // intentionally throwing an error here (not going through `handleFailure`) because
    // this represents a problem with the including package itself that should not be
    // squelchable by a custom `handleFailure`
    let invalidPackageConfigurationMessage = invalidPackageConfiguration.reduce(
      (message, metadata) =>
        `${message}\n\t* ${metadata.name}: ${metadata.reason}`,
      ''
    );

    let relativePath = path.relative(process.cwd(), parentRoot);

    throw new Error(
      `${pkg.name} (at \`./${relativePath}\`) is improperly configured:\n${invalidPackageConfigurationMessage}`
    );
  }

  let isValid = missingPeerDependencies === null && incompatibleRanges === null;

  let result;
  if (isValid) {
    result = true;
  } else {
    result = {
      pkg,
      packagePath,
      incompatibleRanges,
      missingPeerDependencies,
    };
  }

  cache.set(cacheKey, result);

  if (result !== true) {
    handleFailure(result);
  }
};

let AssumptionMapName = '__ValidatePeerDependenciesAssumeProvided';
if (!(AssumptionMapName in global)) {
  global[AssumptionMapName] = new Map();
}

// make sure to re-use the map created by a different instance of
// validate-peer-dependencies
let AssumptionMap = global[AssumptionMapName];

module.exports.assumeProvided = function ({ name, version } = {}) {
  if (name === undefined || version === undefined) {
    throw new Error(
      `assumeProvided({ name, version}): name and version are required, but name='${name}' version='${version}'`
    );
  }
  AssumptionMap.set(name, version);
};

Object.defineProperty(module.exports, '__HasPeerDepsInstalled', {
  enumerable: false,
  configurable: false,
  value: HasPeerDepsInstalled,
});

module.exports._resetCache = function () {
  HasPeerDepsInstalled.clear();
};

module.exports._resetAssumptions = function () {
  global[AssumptionMapName].clear();
};
