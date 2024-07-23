import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/tsdbc.ts", "src/postgres.ts"],
  dts: true,
  splitting: true,
  clean: true,
  minify: true,
  treeshake: true,
  sourcemap: true,
});
