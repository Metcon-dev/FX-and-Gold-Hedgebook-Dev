import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
    // Work around intermittent Windows OneDrive file read errors in React fast refresh.
    plugins: [react({ fastRefresh: false })],
    server: {
        port: 5173,
        proxy: {
            '/api': {
                target: 'http://localhost:5002',
                changeOrigin: true,
                timeout: 600000,       // 10 min – full sync fetches ~13k trades
                proxyTimeout: 600000,  // 10 min – allow long-running syncs
            }
        }
    }
})
