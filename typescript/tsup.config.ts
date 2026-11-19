import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    // Service-owned entry of "./testing": re-exports src/testing/index.ts plus service helpers.
    "testing/index": "src/testing-entry.ts",
    "logs/index": "src/logs/index.ts",
    "traces/index": "src/traces/index.ts",
  },
  format: ["esm", "cjs"],
  outDir: "lib",
  target: "node22",
  platform: "node",
  dts: true,
  sourcemap: true,
  clean: true,
  // Shared chunks keep one copy of each class across entry points (instanceof works).
  splitting: true,
  treeshake: true,
  shims: true,
});
