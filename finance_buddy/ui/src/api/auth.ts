import { request } from './client';

export interface AuthUser {
  id: string;
  username?: string | null;
  display_name?: string | null;
  is_admin?: boolean;
}

export interface PasskeyRegistrationOptionsRequest {
  username?: string;
  display_name?: string;
  invite_token?: string;
}

export interface PasskeyAuthenticationOptionsRequest {
  username?: string;
}

export interface PasskeyOptionsResponse {
  challenge_id: string;
  options: Record<string, unknown>;
}

export interface PasskeyVerifyRequest {
  challenge_id: string;
  credential: Record<string, unknown>;
}

export interface AuthSessionResponse {
  token: string;
  user: AuthUser;
}

export interface UserCreateRequest {
  username: string;
  display_name: string;
}

export interface UserInviteResponse {
  user: AuthUser;
  invite_token: string;
  invite_expires_at: string;
}

export interface UserAdmin {
  id: string;
  username?: string | null;
  display_name?: string | null;
  is_admin: boolean;
  created_at: string;
  passkey_count: number;
}

export interface UserResetResponse extends UserInviteResponse {
  passkeys_revoked: number;
  sessions_revoked: number;
}

export interface SetupStatus {
  needs_setup: boolean;
  has_passkeys: boolean;
  dev_login: boolean;
}

export const getSetupStatus = () => request<SetupStatus>('/auth/setup-status');

export const devLogin = (username?: string) =>
  request<AuthSessionResponse>('/auth/dev-login', {
    method: 'POST',
    body: JSON.stringify(username ? { username } : {})
  });

export const getSession = () => request<AuthUser>('/auth/session');
export const logout = () => request<void>('/auth/logout', { method: 'POST' });

export const beginPasskeyRegistration = (payload: PasskeyRegistrationOptionsRequest) =>
  request<PasskeyOptionsResponse>('/auth/passkeys/registration/options', {
    method: 'POST',
    body: JSON.stringify(payload)
  });

export const verifyPasskeyRegistration = (payload: PasskeyVerifyRequest) =>
  request<{ status: string; credential_id?: string }>('/auth/passkeys/registration/verify', {
    method: 'POST',
    body: JSON.stringify(payload)
  });

export const beginPasskeyAuthentication = (payload: PasskeyAuthenticationOptionsRequest = {}) =>
  request<PasskeyOptionsResponse>('/auth/passkeys/authentication/options', {
    method: 'POST',
    body: JSON.stringify(payload)
  });

export const verifyPasskeyAuthentication = (payload: PasskeyVerifyRequest) =>
  request<AuthSessionResponse>('/auth/passkeys/authentication/verify', {
    method: 'POST',
    body: JSON.stringify(payload)
  });

export const createUserInvite = (payload: UserCreateRequest) =>
  request<UserInviteResponse>('/auth/users', {
    method: 'POST',
    body: JSON.stringify(payload)
  });

export const getUsers = () => request<UserAdmin[]>('/auth/users');

export const resetUserAccess = (userId: string) =>
  request<UserResetResponse>(`/auth/users/${userId}/reset`, {
    method: 'POST'
  });

export const deleteUser = (userId: string) =>
  request<void>(`/auth/users/${userId}`, {
    method: 'DELETE'
  });
