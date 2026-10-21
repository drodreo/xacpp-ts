import { defineConfig } from "@rslib/core";

export default defineConfig({
  lib: [
    {
      format: "esm",
      // Emit a single bundled .d.mts alongside .mjs — TS resolves a .mjs
      // import to its same-named .d.mts declaration file (a plain .d.ts is
      // not matched), and bundling avoids extensionless re-export probing
      // that moduleResolution:bundler cannot satisfy for .d.mts trees.
      dts: { bundle: true, autoExtension: true },
      output: {
        distPath: {
          root: "./dist/esm",
        },
      },
    },
    {
      format: "cjs",
      output: {
        distPath: {
          root: "./dist/cjs",
        },
      },
    },
  ],
  output: {
    target: "node",
  },
});
