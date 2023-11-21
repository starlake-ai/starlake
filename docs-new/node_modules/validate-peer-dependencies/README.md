# validate-peer-dependencies

A utility to allow packages to validate that their specified `peerDependencies` are properly satisified.

## Why?

`peerDependencies` are actually a pretty important mechanism when working with
"plugin systems". For example, most of the packages in the `@babel` namespace
will declare a peer dependency on the version of `@babel/core` that they
require to be present.

Unfortunately, for quite a long time `peerDependencies` were very poorly
supported in the Node ecosystem. Neither `npm` nor `yarn` would automatically
install peer dependencies (`npm@3` `peerDependencies` removed "auto
installation" of `peerDependencies`). They wouldn't even validate that the specified
peer dependency was satisfied (both `npm` and `yarn` would emit a console
warning, which is **very very** often completely ignored).

Finally now with `npm@7` adding back support for installing `peerDependencies`
automatically we are moving in the right direction. Unfortunately, many of us
have projects that must still support older `npm` versions (or `yarn` versions)
that do not provide that installation support.

**That** is where this project comes in. It aims to provide a **_fast_** and **_easy_**
way to validate that your required peer dependencies are satisified.

## Usage

The simplest usage of `validatePeerDependencies` would look like the following:

```js
require('validate-peer-dependencies')(__dirname);
```

This simple invocation will do the following:

* find the nearest `package.json` file from the specified path (in this case `__dirname`)
* read that `package.json` to find any specified `peerDependencies` entries
* ensure that *each* of the specified `peerDependencies` are present and that
  the installed versions match the semver ranges that were specified
* if any of the `peerDependencies` were not present or if their ranges were not satisified
  throw a useful error

Here is an example error message:

```
test-app has the following unmet peerDependencies:

  * bar: `>= 2`; it was not installed
  * foo: `> 1`; it was resolved to `1.0.0`
```

### Known Issues

There are no known scenarios where `validate-peer-dependencies` will flag a
peer dependency as missing, when it really is present. However, there are a few known
problem case where `validate-peer-dependencies` cannot properly validate that a
peer dependency is installed (where a error will not be thrown but it should have been):

To illustrate, let's use the following setup:

* Package `parent` depends on `child` and `sibling`
* Package `child` has a dev dependency (for local development) and a peer
  dependency on `sibling` package
* Package `child` uses `validate-peer-dependencies` to confirm that `sibling` is
  provided

In this case, if `child` has been linked locally (e.g. `npm link`/`yarn link`) into `parent`
when `validate-peer-dependencies` is ran it will incorrectly believe that `parent` has satisfied
the contract, but in fact it _may_ not have. This is a smallish edge case, but still a possible
issue.

These known issues are mitigated by passing in the
`resolvePeerDependenciesFrom` with the root directory of `parent`. As noted in
the documentation for that option below, you often do not have access to the
correct value for `resolvePeerDependenciesFrom` but in some ecosystems (e.g.
ember-cli addons) you **do**. In scenarios where you can use it, you
**absolutely** should.

### Options

A few custom options are available for use:

* `cache` - Can be `false` to disable caching, or a `Map` instance to use your own custom cache
* `handleFailure` - A callback function that will be invoked if validation fails
* `resolvePeerDependenciesFrom` - The path that should be used as the starting point for resolving `peerDependencies` from

#### `cache`

Pass this option to either prevent caching completely (useful in testing
scenarios), or to provide a custom cache.

```js
const validatePeerDependencies = require('validate-peer-dependencies');

// completely disable caching
validatePeerDependencies(__dirname, { cache: false });

// instruct caching system to leverage your own cache
const cache = new Map();
validatePeerDependencies(__dirname, { cache });
```

#### `resolvePeerDependenciesFrom`

Pass this option if you **know** the base directory (the dir containing the
`package.json`) that should be used as the starting point of peer dependency
resolution.

For example, given the following dependencies:

* Package `parent` depends on `child` and `sibling`
* Package `child` has a peer dependency on `sibling` package
* Package `child` uses `validate-peer-dependencies` to confirm that `sibling` is
  provided

_Most_ of the time in the Node ecosystem you can not actually know the path to
`parent` (it could be hoisted / deduplicated to any number of possible
locations), but in some (some what special) circumstances you can. For example,
in the `ember-cli` addon ecosystem an addon is instantiated with access to the
root path of the package that included it (`parent` in the example above).

The main benefit of specifying `resolvePeerDependenciesFrom` is that while
locally developing `child` you might `npm link`/`yarn link` it into `parent`
manually. In that case the default behavior (using the directory that contains
`child`'s `package.json`) is not correct! When linking (and not specifying
`resolvePeerDependenciesFrom`) the invocation to `validatePeerDependencies`
would **always** find the peer dependencies (even if the `parent` didn't have
them installed) because the locally linked copy of `child` would have specified
them in its `devDependencies` and therefore the peer dependency would be
resolvable from `child`'s on disk location.

Here is an example of what usage by an ember-cli addon would look like:

```javascript
'use strict';

const validatePeerDependencies = require('validate-peer-dependencies');

module.exports = {
  // ...snip...
  init() {
    this._super.init.apply(this, arguments);

    validatePeerDependencies(__dirname, {
      resolvePeerDependenciesFrom: this.parent.root,
    });
  }
};
```

Or alternatively, if it only makes sense for the addon to validate peer deps
during a build, that would look like:

```javascript
'use strict';

const validatePeerDependencies = require('validate-peer-dependencies');

module.exports = {
  included(parent) {
    this._super.included.apply(this, arguments);

    validatePeerDependencies(__dirname, {
      resolvePeerDependenciesFrom: parent.root,
    });
    
    return parent;
  }
};
```

#### `handleFailure`

By default, `validatePeerDependencies` emits an error that looks like:

```
test-app has the following unmet peerDependencies:

  * bar: `>= 2`; it was not installed
  * foo: `> 1`; it was resolved to `1.0.0`
```

If you would like to customize the error message (or handle the failure in a
different way), you can provide a custom `handleFailure` callback.

The callback will be passed in a result object with the following interface:

```ts
interface IncompatibleDependency {
  /**
    The name of the package that was incompatible.
  */
  name: string;

  /**
    The peer dependency range that was specified.
  */
  specifiedPeerDependencyRange: string;

  /**
    The version that was actually found.
  */
  version: string;
}

interface MissingPeerDependency {
  /**
    The name of the package that was incompatible.
  */
  name: string;

  /**
    The peer dependency range that was specified.
  */
  specifiedPeerDependencyRange: string;
}

interface Result {
  /**
    The `package.json` contents that were resolved from the specified root
    directory.
  */
  pkg: unknown;

  /**
    The path to the `package.json` that was resolved from the specified root
    directory.
  */
  packagePath: string;

  /**
    The list of peer dependencies that were not found.
  */
  incompatibleRanges: IncompatibleDependency[];

  /**
    The list of peer dependencies that were found, but did not match the
    specified semver range.
  */
  missingPeerDependencies: MissingPeerDependency[];
}
```

For example, this is how you might override the default error message to customize:

```js
validatePeerDependencies(__dirname, {
  handleFailure(result) {
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
  },
});
```

### assumeProvided

It is sometimes desirable to treat a peer dependency as satisfied even when it would not be considered satisfied under the node resolution algorithm.

For example an ember addon may consider itself to satisfy the peer dependency requirements of one of its own dev dependencies during local development.

```js
const assumeProvided = require('validate-peer-dependencies').assumeProvided;

// subsequent calls to validatePeerDependencies will assume some-package is available and will resolve to version 1.2.3
assumeProvided({ name: 'some-package', version: '1.2.3' });

// for the more common case of the package assuming itself to be available during development, the following is the likely preferred invocation
assumeProvided(require('./package.json'));
```

Note that assumptions are global, since peer dependency validation may be occurring in different instances of `validate-peer-dependencies`.

### Disabling checks via Environment Variables

It can be helpful to disable checks when doing certain kinds of testing (e.g. testing a pre-release with breaking changes to see whether any of the changes **actually* break a user).

This can be done with the environment variables `VALIDATE_PEER_DEPENDENCIES` and `IGNORE_PEER_DEPENDENCIES`.

* `VALIDATE_PEER_DEPENDENCIES=false` disables all validation.  Any other value is ignored
* `IGNORE_PEER_DEPENDENCIES=foo,bar` disablesa peer dependency validatation for `foo` and `bar`

## Requirements

[Active versions](https://nodejs.org/en/about/releases/) of Node are supported.

## License

This project is licensed under the [MIT License](LICENSE.md).
