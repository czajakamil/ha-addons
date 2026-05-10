import { useState } from 'react';
import { changePassword } from '../auth';

interface Props {
  onClose: () => void;
}

export function ChangePasswordModal({ onClose }: Props) {
  const [oldPwd, setOldPwd] = useState('');
  const [newPwd, setNewPwd] = useState('');
  const [confirm, setConfirm] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [done, setDone] = useState(false);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    if (newPwd.length < 8) return setError('Nowe hasło musi mieć min. 8 znaków.');
    if (newPwd !== confirm) return setError('Hasła nie są takie same.');
    setBusy(true);
    try {
      await changePassword(oldPwd, newPwd);
      setDone(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Błąd');
    } finally {
      setBusy(false);
    }
  }

  return (
    <div
      role="dialog"
      onClick={onClose}
      style={{
        position: 'fixed',
        inset: 0,
        background: 'rgba(0,0,0,0.4)',
        display: 'grid',
        placeItems: 'center',
        zIndex: 100,
      }}
    >
      <form
        className="auth-card"
        onClick={(e) => e.stopPropagation()}
        onSubmit={onSubmit}
        style={{ maxWidth: 380 }}
      >
        <h1>Zmiana hasła</h1>
        {done ? (
          <>
            <p>Hasło zmienione.</p>
            <button className="primary" type="button" onClick={onClose}>
              OK
            </button>
          </>
        ) : (
          <>
            <label>
              Stare hasło
              <input
                type="password"
                value={oldPwd}
                onChange={(e) => setOldPwd(e.target.value)}
                autoFocus
              />
            </label>
            <label>
              Nowe hasło
              <input
                type="password"
                value={newPwd}
                onChange={(e) => setNewPwd(e.target.value)}
              />
            </label>
            <label>
              Powtórz nowe hasło
              <input
                type="password"
                value={confirm}
                onChange={(e) => setConfirm(e.target.value)}
              />
            </label>
            {error && <div className="auth-error">{error}</div>}
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="primary" type="submit" disabled={busy} style={{ flex: 1 }}>
                {busy ? '...' : 'Zmień hasło'}
              </button>
              <button
                type="button"
                onClick={onClose}
                style={{
                  flex: 1,
                  height: 38,
                  border: '1px solid var(--line)',
                  borderRadius: 8,
                  background: 'transparent',
                  cursor: 'pointer',
                }}
              >
                Anuluj
              </button>
            </div>
          </>
        )}
      </form>
    </div>
  );
}
