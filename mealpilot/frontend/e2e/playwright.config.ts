import { defineConfig, devices } from '@playwright/test';

/**
 * Browser E2E (smoke) dla MealPilot.
 *
 * Samowystarczalny: Playwright sam startuje izolowaną instancję aplikacji
 * (świeża baza, znany admin, zbudowany frontend serwowany przez backend) na
 * porcie 8765 — NIE koliduje z Twoim dev-kontenerem na :8000 i nie potrzebuje
 * Twojego hasła. Wystarczy:
 *
 *   cd frontend
 *   npm install
 *   npm run test:e2e:install   # jednorazowo: pobierz przeglądarkę
 *   npm run test:e2e
 *
 * Wymaga venva backendu (backend/env/bin/python3.12) — tego samego, którym
 * uruchamiasz pytest.
 *
 * Zmienne (opcjonalne): E2E_PORT, E2E_SKIP_BUILD=1 (pomiń build frontu),
 * E2E_BASE_URL (testuj zewnętrzną instancję zamiast startować własną).
 */
const PORT = process.env.E2E_PORT || '8765';
const BASE_URL = process.env.E2E_BASE_URL || `http://127.0.0.1:${PORT}`;
const useOwnServer = !process.env.E2E_BASE_URL;

export default defineConfig({
  testDir: '.',
  timeout: 30_000,
  expect: { timeout: 7_000 },
  fullyParallel: false,
  retries: 0,
  reporter: 'list',
  use: {
    baseURL: BASE_URL,
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  ...(useOwnServer
    ? {
        webServer: {
          command: 'bash run-server.sh',
          url: `${BASE_URL}/healthz`,
          timeout: 120_000,
          reuseExistingServer: false,
          stdout: 'pipe',
          stderr: 'pipe',
        },
      }
    : {}),
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
});
