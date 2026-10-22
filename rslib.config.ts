import { defineConfig } from "@rslib/core";

export default defineConfig({
  lib: [
    {
      format: "esm",
      // The root package.json declares "type": "module", so rslib emits the
      // ESM bundle as .js (the scope default) with a single bundled .d.ts
      // alongside it; the CommonJS sibling below gets an explicit .cjs
      // extension — the extension itself carries the module type, no
      // per-directory package.json needed.
      dts: { bundle: true },
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
