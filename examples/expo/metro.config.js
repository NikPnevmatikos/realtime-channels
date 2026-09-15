// Metro config for consuming the library from the repository root via `file:../..`.
// Not needed when you install realtime-channels from npm.
const path = require('path');
const { getDefaultConfig } = require('expo/metro-config');

const projectRoot = __dirname;
const libraryRoot = path.resolve(projectRoot, '../..');

const config = getDefaultConfig(projectRoot);

// Let Metro see the linked library (dist/) outside the app folder.
config.watchFolders = [libraryRoot];

// `realtime-channels/appsync-events` and `realtime-channels/react` are package "exports".
config.resolver.unstable_enablePackageExports = true;

// The library's own node_modules (dev dependencies, including a copy of React for its tests) must never
// be picked up when bundling files under the library root: always resolve React from the app.
const SINGLETONS = ['react', 'react-native', 'react/jsx-runtime', 'react/jsx-dev-runtime'];
const defaultResolveRequest = config.resolver.resolveRequest;
config.resolver.resolveRequest = (context, moduleName, platform) => {
  if (SINGLETONS.includes(moduleName) && context.originModulePath.startsWith(libraryRoot + path.sep)) {
    const fromApp = { ...context, originModulePath: path.join(projectRoot, 'index.ts') };
    return (defaultResolveRequest ?? context.resolveRequest)(fromApp, moduleName, platform);
  }
  return (defaultResolveRequest ?? context.resolveRequest)(context, moduleName, platform);
};

module.exports = config;
