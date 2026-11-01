# Finance Buddy

Domowy budżet: konta, koperty, paragony z OCR, import z YNAB i CSV z banku.

Kod aplikacji jest rozwijany w osobnym, prywatnym repozytorium. Katalogi `backend/`
i `ui/` oraz pliki `pyproject.toml` i `uv.lock` są tutaj generowane skryptem
`scripts/sync_ha_addon.sh`, więc nie należy ich edytować ręcznie.

## Logowanie

Aplikacja działa wyłącznie przez Ingress i ufa logowaniu Home Assistanta, więc nie
pokazuje ekranu passkey. Każdy, kto ma dostęp do panelu w HA, widzi te same dane jako
użytkownik z opcji `username`. Panel widzą tylko administratorzy HA.

Nie wystawiaj portu aplikacji: `nginx` przyjmuje ruch tylko od bramy Ingress.

## Opcje

| Opcja | Znaczenie |
|-------|-----------|
| `username` | Użytkownik aplikacji. Przy imporcie danych musi zgadzać się z tym w bazie. |
| `ynab_api_key` | Klucz API YNAB. Gdy jest ustawiony, ma pierwszeństwo, a panel nie pokazuje pola na klucz. Pusty, jeśli klucz wpisujesz w aplikacji. |
| `backup_dir` | Katalog automatycznych kopii. Domyślnie w `/share`, poza danymi aplikacji. |
| `receipt_extractor` | Silnik OCR: `rapidocr` (domyślny) albo `tesseract`. |
| `log_level` | Poziom logów. |

`/share` jest widoczny przez dodatek Samba, a kopie zawierają całą historię finansową.
Jeśli udostępniasz ten folder innym osobom, ustaw `backup_dir` na `/data/backups`.

## Przeniesienie istniejących danych

Przy starcie, gdy aplikacja nie ma jeszcze bazy, importuje dane z
`/share/finance_buddy/import/`:

```
finance_buddy/import/finance.sqlite3
finance_buddy/import/attachments/
```

Bazę skopiuj przez `sqlite3 data/finance.sqlite3 ".backup finance.sqlite3"`, nie przez
`cp`, bo działa w trybie WAL. Po imporcie katalog zmienia nazwę na `import.done-*`.
Usuń go z udziału po sprawdzeniu danych.

Import nie nadpisuje istniejącej bazy. Jeśli aplikacja już wystartowała bez danych,
odinstaluj ją (to kasuje `/data`) i zainstaluj ponownie.
