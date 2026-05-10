import { apiFetch } from './data';

export type Role = 'admin' | 'user';

export interface AuthUser {
  id: number;
  username: string;
  role: Role;
  is_active: boolean;
  created_at: string;
}

export interface AdminUser extends AuthUser {}

async function asJson<T>(res: Response): Promise<T> {
  const text = await res.text();
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try {
      const j = text ? (JSON.parse(text) as { detail?: string }) : null;
      if (j?.detail) detail = j.detail;
    } catch {
      /* ignore */
    }
    throw new Error(detail);
  }
  return (text ? JSON.parse(text) : null) as T;
}

export async function fetchSetupRequired(): Promise<boolean> {
  const r = await apiFetch('/auth/setup-required');
  const data = await asJson<{ setup_required: boolean }>(r);
  return data.setup_required;
}

export async function fetchMe(): Promise<AuthUser | null> {
  const r = await apiFetch('/auth/me');
  if (r.status === 401) return null;
  return asJson<AuthUser>(r);
}

export async function login(username: string, password: string): Promise<AuthUser> {
  const r = await apiFetch('/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  });
  return asJson<AuthUser>(r);
}

export async function setupAdmin(username: string, password: string): Promise<AuthUser> {
  const r = await apiFetch('/auth/setup', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  });
  return asJson<AuthUser>(r);
}

export async function logout(): Promise<void> {
  await apiFetch('/auth/logout', { method: 'POST' });
}

export async function changePassword(oldPassword: string, newPassword: string): Promise<void> {
  const r = await apiFetch('/auth/change-password', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ old_password: oldPassword, new_password: newPassword }),
  });
  await asJson<null>(r);
}

export async function listUsers(): Promise<AdminUser[]> {
  return asJson<AdminUser[]>(await apiFetch('/admin/users'));
}

export async function createUser(
  username: string,
  password: string,
  role: Role,
): Promise<AdminUser> {
  return asJson<AdminUser>(
    await apiFetch('/admin/users', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password, role }),
    }),
  );
}

export async function updateUser(
  id: number,
  patch: { username?: string; password?: string; role?: Role; is_active?: boolean },
): Promise<AdminUser> {
  return asJson<AdminUser>(
    await apiFetch(`/admin/users/${id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(patch),
    }),
  );
}

export interface ApiKey {
  id: number;
  name: string;
  prefix: string;
  created_at: string;
  last_used_at: string | null;
}

export interface ApiKeyCreated extends ApiKey {
  key: string;
}

export async function listApiKeys(): Promise<ApiKey[]> {
  return asJson<ApiKey[]>(await apiFetch('/auth/api-keys'));
}

export async function createApiKey(name: string): Promise<ApiKeyCreated> {
  return asJson<ApiKeyCreated>(
    await apiFetch('/auth/api-keys', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    }),
  );
}

export async function deleteApiKey(id: number): Promise<void> {
  const r = await apiFetch(`/auth/api-keys/${id}`, { method: 'DELETE' });
  if (!r.ok) throw new Error(`${r.status}`);
}

export async function deleteUser(id: number): Promise<void> {
  const r = await apiFetch(`/admin/users/${id}`, { method: 'DELETE' });
  if (!r.ok) {
    const text = await r.text();
    let detail = `${r.status}`;
    try {
      const j = text ? (JSON.parse(text) as { detail?: string }) : null;
      if (j?.detail) detail = j.detail;
    } catch {
      /* ignore */
    }
    throw new Error(detail);
  }
}
