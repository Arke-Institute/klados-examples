import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    testTimeout: 600000, // 10 minutes for slow KG extraction
    hookTimeout: 60000,
  },
});
