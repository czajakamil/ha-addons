## 0.2.0
- Agent AI przeniesiony na backend (FastAPI + agent_runner); frontend nie woła już LLM bezpośrednio
- Limity AI: per-user kwoty tokenów i kosztów z resetem miesięcznym
- Gospodstwa domowe: grupowanie użytkowników z dzieleniem przepisów/planów/zakupów
- Własność zasobów: owner_user_id / owner_household_id na przepisach, planach, listach zakupów
- Silniejsze hasła: minimum 12 znaków z literą i cyfrą, walidacja po stronie serwera
- Bezpieczniejsza sesja: session_version z unieważnianiem przy rotacji hasła
- Middleware Cloudflare Access: weryfikacja podpisu JWT (wcześniej tylko sprawdzanie nagłówka)
- Endpoint /estimate-macros: klucz API i URL z env vars (wcześniej per-user w DB)
- Migracje automatyczne: ALTER TABLE przy starcie, bez utraty danych

## 0.1.0
- Pierwsza wersja add-ona: FastAPI backend + React frontend, ingress, SQLite w `/data`.
