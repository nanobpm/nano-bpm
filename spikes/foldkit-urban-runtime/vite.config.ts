import { defineConfig } from "vite";
import { mockUrbanApi } from "./mock/middleware";

// Plain Vite build — Foldkit ships as ESM, no framework plugin needed for a
// production bundle (the Foldkit Vite plugin only adds dev-time HMR). The mock
// middleware stands in for the App-side /app/* endpoints so the spike renders
// headless without a live nano backend.
export default defineConfig({
  plugins: [mockUrbanApi()],
  build: { target: "es2022", minify: "esbuild", reportCompressedSize: true },
});
