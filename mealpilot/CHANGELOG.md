## 0.2.4
- Agent: tytuł rozmowy generowany przez LLM po pierwszej odpowiedzi (zamiast obcinania pierwszej wiadomości na froncie)
- Agent: narzędzia respektują współdzielenie w gospodarstwie domowym (visible_filter w przepisach, planie i zakupach)
- Zakupy: lista auto-generowana regeneruje się automatycznie po usunięciu przepisu (czyszczenie osieroconych pozycji)
- Zakupy API: nowe endpointy `/items/{item_id}` (PATCH/DELETE) niezależne od `week_start`; stare ścieżki pozostają jako aliasy
- Przepisy: `list_recipes` i `filter_recipes` zwracają skróty (bez składników i kroków) — pełne dane przez `get_recipe`
- Przepisy: `create_recipe` przy kolizji ID automatycznie dobiera sufiks (`-2`, `-3`, …) zamiast błędu
- MCP server: rozbudowane opisy narzędzi i schematy parametrów dla klientów zewnętrznych
- Chat UI: auto-resize textarea (do 1/3 wysokości okna), sugestie ukrywane po pierwszej wiadomości, chevron zamiast „X” w przycisku powrotu na mobile
- Wydajność: cache `household_id` w sesji DB (mniej zapytań w przepływie agenta)

## 0.2.3
- Ustawienia: podział na sekcje (Asystent AI / Użytkownicy / Klucze API) z nawigacją bocznymi zakładkami na desktopie i stosem na mobile
- Admin użytkowników: przeprojektowany ekran zarządzania (lepsza prezentacja, edycja kwot, mobilny układ)
- Responsywność: nowy hook `useIsMobile` i dostosowane układy ekranów Plan / Przepisy / Zakupy
- Style: rozbudowane CSS dla ekranu ustawień, lepsze odstępy i karty


