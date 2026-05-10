import { useEffect, useState } from 'react';
import type { AdminUser, Role } from '../auth';
import { createUser, deleteUser, listUsers, updateUser } from '../auth';

interface Props {
  currentUserId: number;
}

export function AdminUsersScreen({ currentUserId }: Props) {
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const [newUsername, setNewUsername] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [newRole, setNewRole] = useState<Role>('user');
  const [showPassword, setShowPassword] = useState(false);
  const [creating, setCreating] = useState(false);

  const [editingId, setEditingId] = useState<number | null>(null);
  const [editUsername, setEditUsername] = useState('');
  const [saving, setSaving] = useState(false);

  async function refresh() {
    setLoading(true);
    try {
      setUsers(await listUsers());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void refresh();
  }, []);

  async function onCreate(e: React.FormEvent) {
    e.preventDefault();
    if (!/^[a-zA-ZąćęłńóśźżĄĆĘŁŃÓŚŹŻ0-9_]+$/.test(newUsername.trim())) {
      setError('Login może zawierać tylko litery, cyfry i podkreślnik.');
      return;
    }
    if (newPassword.length < 8) {
      setError('Hasło musi mieć min. 8 znaków.');
      return;
    }
    setCreating(true);
    try {
      await createUser(newUsername.trim(), newPassword, newRole);
      setNewUsername('');
      setNewPassword('');
      setNewRole('user');
      setShowPassword(false);
      setError(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    } finally {
      setCreating(false);
    }
  }

  async function onResetPassword(u: AdminUser) {
    const pwd = window.prompt(`Nowe hasło dla "${u.username}" (min. 8 znaków):`);
    if (!pwd) return;
    if (pwd.length < 8) {
      setError('Hasło musi mieć min. 8 znaków.');
      return;
    }
    try {
      await updateUser(u.id, { password: pwd });
      setError(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    }
  }

  async function onToggleActive(u: AdminUser) {
    try {
      await updateUser(u.id, { is_active: !u.is_active });
      setError(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    }
  }

  async function onChangeRole(u: AdminUser) {
    const next: Role = u.role === 'admin' ? 'user' : 'admin';
    try {
      await updateUser(u.id, { role: next });
      setError(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    }
  }

  function startEdit(u: AdminUser) {
    setEditingId(u.id);
    setEditUsername(u.username);
    setError(null);
  }

  function cancelEdit() {
    setEditingId(null);
    setEditUsername('');
  }

  async function onSaveUsername(u: AdminUser) {
    const trimmed = editUsername.trim();
    if (!/^[a-zA-ZąćęłńóśźżĄĆĘŁŃÓŚŹŻ0-9_]+$/.test(trimmed)) {
      setError('Login może zawierać tylko litery, cyfry i podkreślnik.');
      return;
    }
    setSaving(true);
    try {
      await updateUser(u.id, { username: trimmed });
      setError(null);
      cancelEdit();
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    } finally {
      setSaving(false);
    }
  }

  async function onDelete(u: AdminUser) {
    if (!window.confirm(`Usunąć użytkownika "${u.username}"? Ta operacja jest nieodwracalna.`))
      return;
    try {
      await deleteUser(u.id);
      setError(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    }
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 28 }}>
      <header className="page-head">
        <div>
          <div className="eyebrow">Administracja</div>
          <h1>Użytkownicy</h1>
          <div className="sub">Zarządzaj kontami i uprawnieniami.</div>
        </div>
      </header>

      {error && <div className="auth-error">{error}</div>}

      <section className="card" style={{ padding: 20 }}>
        <div className="eyebrow" style={{ marginBottom: 12 }}>Dodaj użytkownika</div>
        <form onSubmit={onCreate} className="user-add-form">
          <label className="user-add-field" style={{ flex: '2 1 200px' }}>
            <span className="field-label">Login</span>
            <input
              className="edit-input"
              value={newUsername}
              onChange={(e) => setNewUsername(e.target.value)}
              required
              minLength={1}
              placeholder="np. adam"
            />
          </label>
          <label className="user-add-field" style={{ flex: '2 1 220px' }}>
            <span className="field-label">
              Hasło
              <span className="field-hint">min. 8 znaków</span>
            </span>
            <div className="input-with-affix">
              <input
                className="edit-input"
                type={showPassword ? 'text' : 'password'}
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
                required
                minLength={8}
                placeholder="••••••••"
              />
              <button
                type="button"
                className="btn ghost input-affix-btn"
                onClick={() => setShowPassword((v) => !v)}
                aria-label={showPassword ? 'Ukryj hasło' : 'Pokaż hasło'}
              >
                {showPassword ? 'Ukryj' : 'Pokaż'}
              </button>
            </div>
          </label>
          <label className="user-add-field" style={{ flex: '1 1 120px' }}>
            <span className="field-label">Rola</span>
            <select
              className="edit-input"
              value={newRole}
              onChange={(e) => setNewRole(e.target.value as Role)}
            >
              <option value="user">user</option>
              <option value="admin">admin</option>
            </select>
          </label>
          <div className="user-add-submit">
            <button className="btn primary" type="submit" disabled={creating}>
              {creating ? 'Dodawanie…' : 'Dodaj użytkownika'}
            </button>
          </div>
        </form>
      </section>

      <section>
        <div className="eyebrow" style={{ marginBottom: 12 }}>Lista</div>
        {loading ? (
          <div style={{ color: 'var(--ink-3)' }}>Ładowanie…</div>
        ) : (
          <div className="card" style={{ overflow: 'hidden' }}>
            <div className="table-scroll">
            <table className="admin-users-table">
              <thead>
                <tr>
                  <th>Login</th>
                  <th>Rola</th>
                  <th>Status</th>
                  <th style={{ textAlign: 'right' }}>Akcje</th>
                </tr>
              </thead>
              <tbody>
                {users.map((u) => (
                  <tr key={u.id}>
                    <td style={{ fontWeight: 500 }}>
                      {editingId === u.id ? (
                        <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
                          <input
                            className="edit-input"
                            value={editUsername}
                            onChange={(e) => setEditUsername(e.target.value)}
                            onKeyDown={(e) => {
                              if (e.key === 'Enter') void onSaveUsername(u);
                              if (e.key === 'Escape') cancelEdit();
                            }}
                            autoFocus
                            style={{ minWidth: 0, width: 140 }}
                          />
                          <button className="btn primary" onClick={() => void onSaveUsername(u)} disabled={saving}>
                            {saving ? '…' : 'Zapisz'}
                          </button>
                          <button className="btn" onClick={cancelEdit}>Anuluj</button>
                        </div>
                      ) : (
                        u.username
                      )}
                    </td>
                    <td>
                      <span className={`chip ${u.role === 'admin' ? 'terra' : ''}`}>
                        {u.role}
                      </span>
                    </td>
                    <td>
                      <span className={`chip ${u.is_active ? 'olive' : ''}`}>
                        {u.is_active ? 'aktywny' : 'wyłączony'}
                      </span>
                    </td>
                    <td>
                      <div style={{ display: 'flex', gap: 6, justifyContent: 'flex-end', flexWrap: 'wrap' }}>
                        <button className="btn" onClick={() => startEdit(u)} disabled={editingId !== null}>Edytuj login</button>
                        <button className="btn" onClick={() => onResetPassword(u)}>Zmień hasło</button>
                        <button className="btn" onClick={() => onChangeRole(u)}>
                          {u.role === 'admin' ? 'Zdegraduj' : 'Promuj'}
                        </button>
                        <button className="btn" onClick={() => onToggleActive(u)}>
                          {u.is_active ? 'Dezaktywuj' : 'Aktywuj'}
                        </button>
                        {u.id !== currentUserId && (
                          <button className="btn" onClick={() => onDelete(u)}>Usuń</button>
                        )}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            </div>
          </div>
        )}
      </section>
    </div>
  );
}
