import { defineConfig } from "vite";

export default defineConfig({
  optimizeDeps: { exclude: ["@mina/browser-agent"] },
  worker: {
    format: "es",
  },
});
