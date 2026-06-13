import { test, expect } from '@playwright/test';
import { authenticate } from './helpers';

/**
 * Browser smoke: potwierdza, że UI realnie się renderuje i kluczowe ekrany
 * działają end-to-end (przeglądarka → backend → SQLite). Szczegółowa logika
 * jest pokryta testami API w backend/tests; tu sprawdzamy „czy się składa".
 */

test('logowanie i nawigacja między ekranami', async ({ page }) => {
  await authenticate(page);

  await page.getByRole('button', { name: 'Przepisy' }).click();
  // Demo-przepisy z seedu powinny być widoczne.
  await expect(page.getByText('Kurczak z ryżem', { exact: false })).toBeVisible();

  await page.getByRole('button', { name: 'Lista zakupów' }).click();
  await page.getByRole('button', { name: 'Asystent AI' }).click();
  await page.getByRole('button', { name: 'Plan tygodnia' }).click();
});

test('otwarcie szczegółów przepisu', async ({ page }) => {
  await authenticate(page);
  await page.getByRole('button', { name: 'Przepisy' }).click();

  const firstCard = page.locator('.recipe-card').first();
  await expect(firstCard).toBeVisible();
  await firstCard.click();

  // Klik na kartę otwiera panel szczegółów przepisu.
  await expect(page.locator('.recipe-detail')).toBeVisible();
});

test('start aplikacji bez twardych błędów JS', async ({ page }) => {
  const errors: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error') errors.push(msg.text());
  });

  const resp = await page.goto('/');
  expect(resp?.status()).toBeLessThan(400);
  await authenticate(page);

  // Tolerujemy błędy sieci dla opcjonalnych zasobów, ale nie twarde wyjątki JS.
  const fatal = errors.filter((e) => /Uncaught|is not a function|undefined is not/i.test(e));
  expect(fatal, fatal.join('\n')).toHaveLength(0);
});
