import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  // './' for Home Assistant Ingress, whose path prefix is only known at runtime.
  base: process.env.VITE_BASE || '/',
  // NOTE: no custom manualChunks. Manually splitting React into a separate
  // chunk from its consumers (e.g. @tabler/icons-react) broke module init
  // order in the production build and left a blank screen
  // ("Cannot read properties of undefined (reading 'forwardRef')").
  // Rollup's automatic chunking handles the React interop correctly and still
  // code-splits the lazy-loaded routes.
  server: {
    port: Number(process.env.PORT) || 5173,
    host: '0.0.0.0',
    // Dev-only proxy: lets the browser call the backend same-origin via /__api
    // (see ui/.env.local), sidestepping the backend CORS allow-list during local
    // development. The production nginx build does not use the dev server, so
    // this has no effect there. API_PROXY_TARGET points it at a backend on
    // another port, e.g. when 8000 is taken by a parallel worktree or Docker.
    proxy: {
      '/__api': {
        target: process.env.API_PROXY_TARGET || 'http://localhost:8000',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/__api/, '')
      }
    }
  }
});
