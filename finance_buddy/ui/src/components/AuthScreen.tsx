import { useEffect, useMemo, useState } from 'react';
import {
  IconArrowRight,
  IconFingerprint,
  IconId,
  IconKey,
  IconShieldLock,
  IconSparkles,
  IconUser,
} from '@tabler/icons-react';
import { startAuthentication, startRegistration } from '@simplewebauthn/browser';
import {
  beginPasskeyAuthentication,
  beginPasskeyRegistration,
  devLogin,
  getSetupStatus,
  setAuthToken,
  verifyPasskeyAuthentication,
  verifyPasskeyRegistration,
  type AuthUser,
  type SetupStatus
} from '../api';

type AuthScreenProps = {
  onAuthenticated: (user: AuthUser) => void;
};

const normalizeError = (err: unknown) => {
  if (!(err instanceof Error)) return 'Nie udało się zakończyć operacji.';

  const message = err.message || 'Nie udało się zakończyć operacji.';
  if (message.trim().startsWith('{')) {
    try {
      const parsed = JSON.parse(message) as { detail?: string };
      if (parsed?.detail) return parsed.detail;
    } catch {
      return message;
    }
  }

  return message;
};

const AuthScreen = ({ onAuthenticated }: AuthScreenProps) => {
  const [displayName, setDisplayName] = useState('');
  const [username, setUsername] = useState('');
  const [inviteToken, setInviteToken] = useState('');
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [setup, setSetup] = useState<SetupStatus | null>(null);

  const passkeySupported = useMemo(
    () => typeof window !== 'undefined' && 'PublicKeyCredential' in window,
    []
  );

  const refreshSetup = async () => {
    try {
      setSetup(await getSetupStatus());
    } catch {
      // If the status check fails we just fall back to the default UI.
      setSetup(null);
    }
  };

  useEffect(() => {
    void refreshSetup();
  }, []);

  // No passkeys exist yet (fresh install) -> logging in cannot work, the user
  // must create the first account instead.
  const needsFirstAccount = setup ? !setup.has_passkeys : false;
  const isFirstAdmin = setup ? setup.needs_setup : false;

  const handleRegister = async () => {
    if (!passkeySupported) {
      setError('Twoja przeglądarka nie wspiera passkeys.');
      return;
    }

    // A brand-new account (the first admin) must carry a display name. Invite-based
    // registration reuses an existing account, so the field is optional there.
    if (isFirstAdmin && !displayName.trim()) {
      setStatus(null);
      setError('Nazwa wyświetlana jest wymagana przy tworzeniu nowego konta.');
      return;
    }

    setBusy(true);
    setError(null);
    setStatus('Tworzę nowy passkey…');

    try {
      const { options, challenge_id } = await beginPasskeyRegistration({
        username: username.trim() || undefined,
        display_name: displayName.trim() || undefined,
        invite_token: inviteToken.trim() || undefined
      });
      const attestation = await startRegistration(options as any);
      await verifyPasskeyRegistration({ challenge_id, credential: attestation as any });
      setStatus('Passkey dodany. Możesz się teraz zalogować.');
      void refreshSetup();
    } catch (err) {
      setStatus(null);
      setError(normalizeError(err));
    } finally {
      setBusy(false);
    }
  };

  const handleLogin = async () => {
    if (!passkeySupported) {
      setError('Twoja przeglądarka nie wspiera passkeys.');
      return;
    }

    if (needsFirstAccount) {
      setStatus(null);
      setError(
        'Brak zapisanych passkeys. Najpierw utwórz konto (panel „Dodaj passkey” po prawej).'
      );
      return;
    }

    setBusy(true);
    setError(null);
    setStatus('Otwieram logowanie passkey…');

    try {
      const { options, challenge_id } = await beginPasskeyAuthentication();
      const assertion = await startAuthentication(options as any);
      const session = await verifyPasskeyAuthentication({
        challenge_id,
        credential: assertion as any
      });
      setAuthToken(session.token);
      setStatus('Zalogowano pomyślnie.');
      onAuthenticated(session.user);
    } catch (err) {
      setStatus(null);
      setError(normalizeError(err));
    } finally {
      setBusy(false);
    }
  };

  const handleDevLogin = async () => {
    setBusy(true);
    setError(null);
    setStatus('Logowanie deweloperskie…');

    try {
      const session = await devLogin(username.trim() || undefined);
      setAuthToken(session.token);
      setStatus('Zalogowano (tryb deweloperski).');
      onAuthenticated(session.user);
    } catch (err) {
      setStatus(null);
      setError(normalizeError(err));
    } finally {
      setBusy(false);
    }
  };

  const devLoginEnabled = setup?.dev_login ?? false;

  return (
    <div className="welcome">
      <div className="welcome__halo" aria-hidden="true" />
      <div className="welcome__panel" style={{ width: 'min(980px, 100%)' }}>
        <div className="welcome__header">
          <div className="welcome__eyebrow">Budget Buddy</div>
          <h1>Zaloguj się passkey</h1>
          <p className="welcome__lead">
            Bez haseł, bez SMS-ów. Wystarczy potwierdzenie na urządzeniu — odcisk palca, Face ID
            albo PIN.
          </p>
        </div>

        {!passkeySupported && (
          <div className="welcome__alert welcome__alert--error" role="alert">
            Ta przeglądarka nie obsługuje passkeys. Użyj nowszego Chrome, Safari lub Edge.
          </div>
        )}

        {passkeySupported && needsFirstAccount && (
          <div className="welcome__card welcome__card--accent" style={{ margin: 0 }}>
            <header className="welcome__card-header">
              <div className="welcome__icon welcome__icon--accent">
                <IconSparkles size={22} stroke={1.6} />
              </div>
              <div>
                <p className="welcome__card-eyebrow">
                  {isFirstAdmin ? 'Pierwsze uruchomienie' : 'Brak passkey na tym urządzeniu'}
                </p>
                <h2>
                  {isFirstAdmin
                    ? 'Utwórz konto admina'
                    : 'Najpierw dodaj passkey'}
                </h2>
              </div>
            </header>
            <p className="welcome__text">
              {isFirstAdmin
                ? 'Nie ma jeszcze żadnego użytkownika, więc logowanie nie ma czego sprawdzić. Wpisz login admina w panelu „Dodaj passkey” i utwórz pierwszy passkey.'
                : 'Logowanie nie zadziała, dopóki na tym urządzeniu nie utworzysz passkey. Skorzystaj z panelu „Dodaj passkey” (potrzebny kod zaproszenia od admina).'}
            </p>
          </div>
        )}

        <div className="welcome__grid">
          <section className="welcome__card welcome__card--primary">
            <header className="welcome__card-header">
              <div className="welcome__icon">
                <IconShieldLock size={22} stroke={1.6} />
              </div>
              <div>
                <p className="welcome__card-eyebrow">Mam już passkey</p>
                <h2>Logowanie</h2>
              </div>
            </header>

            <p className="welcome__text">
              {needsFirstAccount
                ? 'Brak zapisanych passkeys — najpierw utwórz konto w panelu obok.'
                : 'Wybierz zapisany passkey i potwierdź logowanie na swoim urządzeniu.'}
            </p>

            <div className="welcome__actions" style={{ marginTop: 'auto' }}>
              <button
                type="button"
                className="btn btn-primary d-inline-flex align-items-center gap-2"
                onClick={handleLogin}
                disabled={busy || !passkeySupported || needsFirstAccount}
              >
                <IconFingerprint size={18} aria-hidden="true" />
                {busy ? 'Otwieram…' : 'Zaloguj się passkey'}
              </button>

              {devLoginEnabled && (
                <button
                  type="button"
                  className="btn btn-outline-secondary d-inline-flex align-items-center gap-2"
                  onClick={handleDevLogin}
                  disabled={busy}
                  title="Logowanie deweloperskie — pomija passkey (tylko środowisko dev)"
                >
                  <IconKey size={18} aria-hidden="true" />
                  {busy ? 'Loguję…' : 'Logowanie deweloperskie'}
                </button>
              )}
            </div>
          </section>

          <section className="welcome__card welcome__card--accent">
            <header className="welcome__card-header">
              <div className="welcome__icon welcome__icon--accent">
                <IconKey size={22} stroke={1.6} />
              </div>
              <div>
                <p className="welcome__card-eyebrow">Pierwsza konfiguracja</p>
                <h2>Dodaj passkey</h2>
              </div>
            </header>

            <p className="welcome__text">
              Użyj zaproszenia od admina lub skonfiguruj pierwsze konto admina.
            </p>

            <label className="welcome__label" htmlFor="auth-invite">
              Kod zaproszenia (opcjonalnie)
            </label>
            <div className="welcome__control">
              <div className="welcome__input-icon" aria-hidden="true">
                <IconSparkles size={16} stroke={1.6} />
              </div>
              <input
                id="auth-invite"
                value={inviteToken}
                onChange={(event) => setInviteToken(event.target.value)}
                placeholder="np. INVITE-123"
                disabled={busy}
              />
            </div>

            <label className="welcome__label" htmlFor="auth-username">
              Login / username (dla pierwszego admina)
            </label>
            <div className="welcome__control">
              <div className="welcome__input-icon" aria-hidden="true">
                <IconUser size={16} stroke={1.6} />
              </div>
              <input
                id="auth-username"
                value={username}
                onChange={(event) => setUsername(event.target.value)}
                placeholder="np. admin"
                disabled={busy}
              />
            </div>

            <label className="welcome__label" htmlFor="auth-displayname">
              {isFirstAdmin ? 'Nazwa wyświetlana (wymagana)' : 'Nazwa wyświetlana (opcjonalnie)'}
            </label>
            <div className="welcome__control">
              <div className="welcome__input-icon" aria-hidden="true">
                <IconId size={16} stroke={1.6} />
              </div>
              <input
                id="auth-displayname"
                value={displayName}
                onChange={(event) => setDisplayName(event.target.value)}
                placeholder="np. Kamil"
                disabled={busy}
                required={isFirstAdmin}
                aria-required={isFirstAdmin}
              />
            </div>

            <div className="welcome__actions" style={{ marginTop: 'auto' }}>
              <button
                type="button"
                className="btn btn-primary d-inline-flex align-items-center gap-2"
                onClick={handleRegister}
                disabled={busy || !passkeySupported || (isFirstAdmin && !displayName.trim())}
              >
                {busy ? 'Tworzę…' : 'Utwórz passkey'}
                <IconArrowRight size={18} aria-hidden="true" />
              </button>
            </div>
          </section>
        </div>

        {(status || error) && (
          <div
            className={`welcome__alert ${error ? 'welcome__alert--error' : 'welcome__alert--success'}`}
            role={error ? 'alert' : 'status'}
            aria-live="polite"
          >
            {error ?? status}
          </div>
        )}

        <div className="welcome__footer">
          <div className="welcome__trust">
            <IconShieldLock size={16} aria-hidden="true" />
            <span>
              Passkeys są przechowywane bezpiecznie na Twoim urządzeniu. Nie wysyłamy ani nie
              zapisujemy żadnych haseł.
            </span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AuthScreen;
