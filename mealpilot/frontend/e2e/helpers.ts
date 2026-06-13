import { Page, expect } from '@playwright/test';

// Domyślne dane logowania muszą zgadzać się z adminem provisionowanym w e2e/run-server.sh.
export const USERNAME = process.env.E2E_USERNAME || 'e2e_admin';
export const PASSWORD = process.env.E2E_PASSWORD || 'E2eAdminPass1234';

/**
 * Doprowadza stronę do stanu zalogowanego. Obsługuje obie ścieżki:
 *  - świeża instancja → ekran "Załóż konto admina" (/setup),
 *  - istniejąca instancja → ekran "Zaloguj się".
 */
export async function authenticate(page: Page) {
  await page.goto('/');

  const setupHeading = page.getByRole('heading', { name: 'Załóż konto admina' });
  const loginHeading = page.getByRole('heading', { name: 'Zaloguj się' });

  await expect(setupHeading.or(loginHeading)).toBeVisible();

  if (await setupHeading.isVisible()) {
    await page.getByLabel('Login').fill(USERNAME);
    await page.getByLabel('Hasło', { exact: true }).fill(PASSWORD);
    await page.getByLabel('Powtórz hasło').fill(PASSWORD);
    await page.getByRole('button', { name: 'Utwórz konto' }).click();
  } else {
    await page.getByLabel('Login').fill(USERNAME);
    await page.getByLabel('Hasło', { exact: true }).fill(PASSWORD);
    await page.getByRole('button', { name: 'Zaloguj' }).click();
  }

  // Po zalogowaniu pojawia się nawigacja aplikacji.
  await expect(page.getByRole('button', { name: 'Przepisy' })).toBeVisible();
}
