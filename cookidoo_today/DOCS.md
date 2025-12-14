# Cookidoo Today

Add-on wystawia API:

- `/api/today` – dzisiejsze przepisy
- `/api/week` – cały tydzień
- `/api/image/<recipe_id>.jpg` – obrazek przepisu (lokalny cache w /data/images)
- `/api/day/<YYYY-MM-DD>.jpg` – kolaż dzienny (jeśli zbudowany)
- `/api/week.jpg` – kolaż tygodniowy

Konfiguracja add-ona jest w UI Home Assistant i trafia do `/data/options.json`.
