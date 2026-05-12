import { useEffect, useState } from 'react';
import type { AdminUser, Household, Role } from '../auth';
import {
  assignUserToHousehold,
  createHousehold,
  createUser,
  deleteHousehold,
  deleteUser,
  listHouseholds,
  listUsers,
  renameHousehold,
  resetAiUsage,
  updateAiLimits,
  updateUser,
} from '../auth';

interface Props {
  currentUserId: number;
}

export function AdminUsersScreen({ currentUserId }: Props) {
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [households, setHouseholds] = useState<Household[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [newHouseholdName, setNewHouseholdName] = useState('');

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
      const [us, hs] = await Promise.all([listUsers(), listHouseholds()]);
      setUsers(us);
      setHouseholds(hs);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    } finally {
      setLoading(false);
    }
  }

  async function onCreateHousehold(e: React.FormEvent) {
    e.preventDefault();
    const name = newHouseholdName.trim();
    if (!name) return;
    try {
      await createHousehold(name);
      setNewHouseholdName('');
      setError(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    }
  }

  async function onRenameHousehold(h: Household) {
    const name = window.prompt(`Nowa nazwa dla "${h.name}":`, h.name);
    if (!name || name.trim() === h.name) return;
    try {
      await renameHousehold(h.id, name.trim());
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    }
  }

  async function onDeleteHousehold(h: Household) {
    if (!window.confirm(
      `Usunąć household "${h.name}"? Współdzielone przepisy/szablony wrócą do twórców jako personal.`
    )) return;
    try {
      await deleteHousehold(h.id);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    }
  }

  async function onAssignHousehold(u: AdminUser, householdId: number | null) {
    try {
      await assignUserToHousehold(u.id, householdId, u.can_edit_in_household);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    }
  }

  async function onToggleCanEdit(u: AdminUser) {
    if (u.household_id == null) return;
    try {
      await assignUserToHousehold(u.id, u.household_id, !u.can_edit_in_household);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    }
  }

  async function onToggleCanUseAi(u: AdminUser) {
    try {
      await updateAiLimits(u.id, { can_use_ai: !u.can_use_ai });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    }
  }

  async function onSetTokenLimit(u: AdminUser) {
    const input = window.prompt(
      `Miesięczny limit tokenów dla "${u.username}" (pusty = brak limitu):`,
      u.ai_monthly_token_limit?.toString() ?? '',
    );
    if (input === null) return;
    const trimmed = input.trim();
    try {
      if (trimmed === '') {
        await updateAiLimits(u.id, { clear_token_limit: true });
      } else {
        const n = parseInt(trimmed, 10);
        if (!Number.isFinite(n) || n < 0) {
          setError('Limit musi być liczbą nieujemną.');
          return;
        }
        await updateAiLimits(u.id, { ai_monthly_token_limit: n });
      }
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    }
  }

  async function onSetCostLimit(u: AdminUser) {
    const currentDollars = u.ai_monthly_cost_limit_cents != null
      ? (u.ai_monthly_cost_limit_cents / 100).toString()
      : '';
    const input = window.prompt(
      `Miesięczny limit kosztów dla "${u.username}" w USD (pusty = brak limitu):`,
      currentDollars,
    );
    if (input === null) return;
    const trimmed = input.trim();
    try {
      if (trimmed === '') {
        await updateAiLimits(u.id, { clear_cost_limit: true });
      } else {
        const f = parseFloat(trimmed);
        if (!Number.isFinite(f) || f < 0) {
          setError('Limit musi być liczbą nieujemną.');
          return;
        }
        await updateAiLimits(u.id, { ai_monthly_cost_limit_cents: Math.round(f * 100) });
      }
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    }
  }

  async function onResetUsage(u: AdminUser) {
    if (!window.confirm(`Zresetować licznik AI dla "${u.username}"?`)) return;
    try {
      await resetAiUsage(u.id);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
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

  async function onChangeRole(u: AdminUser, next: Role) {
    if (next === u.role) return;
    try {
      await updateUser(u.id, { role: next });
      setError(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    }
  }

  async function onChangeActive(u: AdminUser, next: boolean) {
    if (next === u.is_active) return;
    try {
      await updateUser(u.id, { is_active: next });
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

      <section className="card" style={{ padding: 20 }}>
        <div className="eyebrow" style={{ marginBottom: 12 }}>Households (gospodarstwa)</div>
        <form onSubmit={onCreateHousehold} style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
          <input
            className="edit-input"
            value={newHouseholdName}
            onChange={(e) => setNewHouseholdName(e.target.value)}
            placeholder="Nazwa, np. Rodzina Kowalskich"
            style={{ flex: 1 }}
          />
          <button className="btn primary" type="submit">Utwórz</button>
        </form>
        {households.length === 0 ? (
          <div style={{ color: 'var(--ink-3)' }}>Brak — utwórz pierwsze gospodarstwo powyżej.</div>
        ) : (
          <ul style={{ listStyle: 'none', padding: 0, margin: 0, display: 'flex', flexDirection: 'column', gap: 6 }}>
            {households.map((h) => (
              <li key={h.id} style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '8px 0', borderTop: '1px solid var(--line)' }}>
                <strong style={{ flex: 1 }}>{h.name}</strong>
                <span className="chip">{h.member_count} {h.member_count === 1 ? 'członek' : 'członków'}</span>
                <button className="btn" onClick={() => onRenameHousehold(h)}>Zmień nazwę</button>
                <button className="btn" onClick={() => onDeleteHousehold(h)}>Usuń</button>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section>
        <div className="eyebrow" style={{ marginBottom: 12 }}>Lista użytkowników</div>
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
                  <th>Household</th>
                  <th>AI</th>
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
                      <select
                        className={`chip-select ${u.role === 'admin' ? 'terra' : ''}`}
                        value={u.role}
                        onChange={(e) => void onChangeRole(u, e.target.value as Role)}
                        disabled={u.id === currentUserId}
                        title={u.id === currentUserId ? 'Nie można zmienić własnej roli' : 'Zmień rolę'}
                      >
                        <option value="user">user</option>
                        <option value="admin">admin</option>
                      </select>
                    </td>
                    <td>
                      <select
                        className={`chip-select ${u.is_active ? 'olive' : ''}`}
                        value={u.is_active ? 'active' : 'inactive'}
                        onChange={(e) => void onChangeActive(u, e.target.value === 'active')}
                        disabled={u.id === currentUserId}
                        title={u.id === currentUserId ? 'Nie można zmienić własnego statusu' : 'Zmień status'}
                      >
                        <option value="active">aktywny</option>
                        <option value="inactive">wyłączony</option>
                      </select>
                    </td>
                    <td>
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                        <select
                          className="edit-input"
                          value={u.household_id ?? ''}
                          onChange={(e) => {
                            const v = e.target.value;
                            void onAssignHousehold(u, v === '' ? null : parseInt(v, 10));
                          }}
                          style={{ minWidth: 120 }}
                        >
                          <option value="">— brak —</option>
                          {households.map((h) => (
                            <option key={h.id} value={h.id}>{h.name}</option>
                          ))}
                        </select>
                        {u.household_id != null && (
                          <label style={{ fontSize: 12, display: 'flex', alignItems: 'center', gap: 4 }}>
                            <input
                              type="checkbox"
                              checked={u.can_edit_in_household}
                              onChange={() => void onToggleCanEdit(u)}
                            />
                            edytor
                          </label>
                        )}
                      </div>
                    </td>
                    <td>
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 2, fontSize: 12 }}>
                        <label style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                          <input
                            type="checkbox"
                            checked={u.can_use_ai}
                            onChange={() => void onToggleCanUseAi(u)}
                          />
                          włączone
                        </label>
                        <div style={{ color: 'var(--ink-3)' }}>
                          tok: {u.ai_used_tokens_this_month}{u.ai_monthly_token_limit != null ? ` / ${u.ai_monthly_token_limit}` : ''}
                        </div>
                        <div style={{ color: 'var(--ink-3)' }}>
                          $: {(u.ai_used_cost_cents_this_month / 100).toFixed(2)}{u.ai_monthly_cost_limit_cents != null ? ` / ${(u.ai_monthly_cost_limit_cents / 100).toFixed(2)}` : ''}
                        </div>
                      </div>
                    </td>
                    <td>
                      <div style={{ display: 'flex', gap: 6, justifyContent: 'flex-end', flexWrap: 'wrap' }}>
                        <button className="btn" onClick={() => onSetTokenLimit(u)}>Limit tok.</button>
                        <button className="btn" onClick={() => onSetCostLimit(u)}>Limit $</button>
                        <button className="btn" onClick={() => onResetUsage(u)}>Reset AI</button>
                        <button className="btn" onClick={() => startEdit(u)} disabled={editingId !== null}>Edytuj login</button>
                        <button className="btn" onClick={() => onResetPassword(u)}>Zmień hasło</button>
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
