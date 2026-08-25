import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 5021,
    strictPort: true,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:3021',
        changeOrigin: true,
      },
      '/ws': {
        target: 'ws://127.0.0.1:3021',
        ws: true,
      },
    },
  },
})
