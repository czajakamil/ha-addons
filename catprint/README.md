# CatPrint Add-on

Serwer REST do drukowania na termicznej drukarce BLE – GOTOOGO C15 / MXW01 (protokół FunPrint).

## Funkcje

- **Web UI** – panel do drukowania tekstu, obrazów, QR i list zakupów
- **REST API** – integracja z Home Assistant (serwisy, notify, automatyzacje)
- **Kolejka wydruków** – gdy drukarka jest wyłączona, zadania są zapisywane w bazie i automatycznie drukowane po jej włączeniu
- **Auto-wykrywanie BLE** – skanuje co 10 sekund, łączy się automatycznie

## Konfiguracja

| Opcja | Opis |
|---|---|
| `printer_address` | Adres MAC drukarki BLE (opcjonalnie – add-on sam ją znajdzie) |
| `printer_name` | Nazwa drukarki BLE (opcjonalnie) |

## Użycie z HA

Zainstaluj też integrację **CatPrint** z repo `ha-custom_components` i podaj URL:
`http://homeassistant.local:5123`

### Przykładowa automatyzacja

```yaml
automation:
  trigger:
    platform: state
    entity_id: binary_sensor.door
    to: "on"
  action:
    service: catprint.print_notification
    data:
      title: "Drzwi otwarte"
      message: "Drzwi wejściowe zostały otwarte."
```
