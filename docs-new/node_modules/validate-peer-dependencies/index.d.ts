type Options = {
  /**
     Set to `false` to disabling caching or provide a `Map` to control cache
     sharing. The contents of the map are opaque.
   */
  cache?: boolean | Map<unknown, unknown>;
  /**
    Specify a custom handler for errors. This can be useful to provide better
    errors for your particular context.
   */
  handleFailure?: (result: unknown) => void;
  /**
    The path that should be used as the starting point for resolving
    `peerDependencies` from
  */
  resolvePeerDependenciesFrom?: string;
};

declare const validatePeerDependencies: {
  /**
    Validate peer dependencies for the package rooted at `parentRoot`.

    @example
    ```typescript
      validatePeerDependencies(__dirname);
    ```

    @param parentRoot -
    The directory containing the `package.json` to validate. Typically this
    should be specified via `__dirname`.

    @param [options]
    @param {boolean | Map<unkown,unknown>} options.cache -
      Set to `false` to disabling caching or provide a `Map` to control cache
      sharing. The contents of the map are opaque.
    @param [options.handleFailure] -
      Specify a custom handler for errors. This can be useful to provide better
      errors for your particular context.
    @param [options.resolvePeerDependenciesFrom] -
      The path that should be used as the starting point for resolving
      `peerDependencies` from.

  */
  (parentRoot: string, options?: Options): void;

  /**
    Treat `pkg` as being present, overriding whatever is found in `node_modules`.

    This is often useful during development when linking to a library that has peer dependencies.
   */
  assumeProvided(pkg: { name?: string; version?: string }): void;
};

export = validatePeerDependencies;
