import { defineConfig } from 'tsup';

export default defineConfig([
  {
    entry: {
      index: 'src/core/index.ts',
      'appsync-events': 'src/appsync-events/index.ts',
      react: 'src/react/index.tsx',
    },
    format: ['esm', 'cjs'],
    dts: true,
    sourcemap: true,
    clean: true,
    target: 'es2020',
    external: ['react'],
    treeshake: true,
  },
  {
    // Single-file browser bundle (core + AppSync Events adapter) for <script> usage and the examples.
    entry: { 'realtime-channels.appsync': 'src/iife.ts' },
    format: ['iife'],
    globalName: 'RealtimeChannels',
    minify: true,
    sourcemap: true,
    target: 'es2020',
    outExtension: () => ({ js: '.global.js' }),
  },
]);
