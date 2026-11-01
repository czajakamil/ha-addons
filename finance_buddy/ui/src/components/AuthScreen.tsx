import { useEffect, useMemo, useState } from 'react';
import { ChevronDownIcon, FingerprintIcon, KeyIcon } from 'lucide-react';
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
  const [showSetup, setShowSetup] = useState(false);

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
        'Brak zapisanych passkeys. Najpierw utwórz konto.'
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
    <main className="auth">
      <div className="auth__panel">
        <p className="auth__brand">Budget Buddy</p>
        <h1 className="auth__title">{needsFirstAccount ? (isFirstAdmin ? 'Utwórz konto admina' : 'Dodaj passkey') : 'Zaloguj się'}</h1>
        <p className="auth__lead">
          {needsFirstAccount
            ? isFirstAdmin
              ? 'To pierwsze uruchomienie, więc nie ma jeszcze żadnego użytkownika.'
              : 'Na tym urządzeniu nie ma passkey. Potrzebny będzie kod zaproszenia od admina.'
            : 'Bez hasła. Wystarczy odcisk palca, Face ID albo PIN.'}
        </p>

        {!passkeySupported && (
          <p className="auth__message auth__message--error" role="alert">
            Ta przeglądarka nie obsługuje passkeys. Użyj nowszego Chrome, Safari lub Edge.
          </p>
        )}

        {!needsFirstAccount && (
          <button
            type="button"
            className="btn btn-primary auth__submit"
            onClick={handleLogin}
            disabled={busy || !passkeySupported}
          >
            <FingerprintIcon size={18} aria-hidden="true" />
            {busy ? 'Otwieram…' : 'Zaloguj się passkey'}
          </button>
        )}

        {devLoginEnabled && (
          <button
            type="button"
            className="btn btn-outline-secondary auth__submit"
            onClick={handleDevLogin}
            disabled={busy}
            title="Pomija passkey, tylko środowisko deweloperskie"
          >
            <KeyIcon size={18} aria-hidden="true" />
            {busy ? 'Loguję…' : 'Logowanie deweloperskie'}
          </button>
        )}

        {!needsFirstAccount && (
          <button
            type="button"
            className="text-button auth__disclosure"
            aria-expanded={showSetup}
            aria-controls="auth-setup"
            onClick={() => setShowSetup((current) => !current)}
          >
            <ChevronDownIcon size={14} aria-hidden="true" className={showSetup ? 'is-open' : undefined} />
            Mam zaproszenie, dodaj passkey
          </button>
        )}

        {(needsFirstAccount || showSetup) && (
          <form
            id="auth-setup"
            className="auth__form"
            onSubmit={(event) => {
              event.preventDefault();
              void handleRegister();
            }}
          >
            {!isFirstAdmin && (
              <div>
                <label className="form-label" htmlFor="auth-invite">
                  Kod zaproszenia
                </label>
                <input
                  id="auth-invite"
                  className="form-control"
                  value={inviteToken}
                  onChange={(event) => setInviteToken(event.target.value)}
                  placeholder="np. INVITE-123"
                  autoComplete="off"
                  disabled={busy}
                />
              </div>
            )}
            {isFirstAdmin && (
              <div>
                <label className="form-label" htmlFor="auth-username">
                  Login
                </label>
                <input
                  id="auth-username"
                  className="form-control"
                  value={username}
                  onChange={(event) => setUsername(event.target.value)}
                  placeholder="np. admin"
                  autoComplete="username webauthn"
                  disabled={busy}
                />
              </div>
            )}
            <div>
              <label className="form-label" htmlFor="auth-displayname">
                {isFirstAdmin ? 'Nazwa wyświetlana' : 'Nazwa wyświetlana (opcjonalnie)'}
              </label>
              <input
                id="auth-displayname"
                className="form-control"
                value={displayName}
                onChange={(event) => setDisplayName(event.target.value)}
                placeholder="np. Kamil"
                autoComplete="name"
                disabled={busy}
                required={isFirstAdmin}
              />
            </div>
            <button
              type="submit"
              className={`btn ${needsFirstAccount ? 'btn-primary' : 'btn-outline-secondary'} auth__submit`}
              disabled={busy || !passkeySupported || (isFirstAdmin && !displayName.trim())}
            >
              {busy ? 'Tworzę…' : 'Utwórz passkey'}
            </button>
          </form>
        )}

        {(status || error) && (
          <p
            className={`auth__message ${error ? 'auth__message--error' : ''}`}
            role={error ? 'alert' : 'status'}
          >
            {error ?? status}
          </p>
        )}
      </div>
    </main>
  );
};

export default AuthScreen;
