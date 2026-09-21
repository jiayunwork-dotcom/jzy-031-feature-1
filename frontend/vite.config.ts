import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// During development Vite proxies API calls to the Go gateway; in production
// nginx serves the built assets and proxies /api to the backend.
export default defineConfig({
  plugins: [vue()],
  server: {
    host: true,
    port: 5173,
    proxy: {
      '/api': {
        target: process.env.VITE_API_TARGET || 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
})
