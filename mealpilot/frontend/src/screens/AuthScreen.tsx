import { useState } from 'react';
import type { AuthUser } from '../auth';
import { login, setupAdmin } from '../auth';

interface Props {
  mode: 'setup' | 'login';
  onAuthenticated: (user: AuthUser) => void;
}

export function AuthScreen({ mode, onAuthenticated }: Props) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [confirm, setConfirm] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const isSetup = mode === 'setup';

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    if (username.trim().length < 3) {
      setError('Login musi mieć co najmniej 3 znaki.');
      return;
    }
    if (isSetup) {
      if (password.length < 12 || !/[a-zA-Z]/.test(password) || !/[0-9]/.test(password)) {
        setError('Hasło musi mieć co najmniej 12 znaków i zawierać co najmniej jedną literę i jedną cyfrę.');
        return;
      }
      if (password !== confirm) {
        setError('Hasła nie są takie same.');
        return;
      }
    }

    setBusy(true);
    try {
      const user = isSetup
        ? await setupAdmin(username.trim(), password)
        : await login(username.trim(), password);
      onAuthenticated(user);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Coś poszło nie tak.');
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="auth-screen">
      <form className="auth-card" onSubmit={onSubmit}>
        <h1>{isSetup ? 'Załóż konto admina' : 'Zaloguj się'}</h1>
        <p className="auth-sub">
          {isSetup
            ? 'Pierwsze uruchomienie — wybierz login i hasło administratora.'
            : 'MealPilot'}
        </p>

        <label>
          Login
          <input
            type="text"
            autoComplete="username"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoFocus
          />
        </label>
        <label>
          Hasło
          <input
            type="password"
            autoComplete={isSetup ? 'new-password' : 'current-password'}
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
        </label>
        {isSetup && (
          <label>
            Powtórz hasło
            <input
              type="password"
              autoComplete="new-password"
              value={confirm}
              onChange={(e) => setConfirm(e.target.value)}
            />
          </label>
        )}

        {error && <div className="auth-error">{error}</div>}

        <button className="primary" type="submit" disabled={busy}>
          {busy ? '...' : isSetup ? 'Utwórz konto' : 'Zaloguj'}
        </button>
      </form>
    </div>
  );
}
