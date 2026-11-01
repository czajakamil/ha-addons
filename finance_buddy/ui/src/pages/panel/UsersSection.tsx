// Użytkownicy (tylko admin): lista, zaproszenia, reset dostępu i usuwanie konta.
import { useEffect, useRef, useState, type FormEvent } from 'react';
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import { Copy, Ellipsis, RefreshCw, UserPlus, X } from 'lucide-react';
import { toast } from 'sonner';
import {
  createUserInvite,
  deleteUser,
  getUsers,
  resetUserAccess,
  type UserAdmin,
  type UserInviteResponse
} from '../../api';
import ResizableTable from '../../components/ui/ResizableTable';
import StoreDialog from '../stores/components/StoreDialog';
import { errorDetail, formatDate, formatDateTime } from './format';

const NEW_USER_FORM = 'panel-new-user-form';

const userLabel = (user: UserAdmin) => user.display_name || user.username || 'Użytkownik';

type NewUserDialogProps = {
  onClose: () => void;
  onCreated: (invite: UserInviteResponse) => void;
};

const NewUserDialog = ({ onClose, onCreated }: NewUserDialogProps) => {
  const [username, setUsername] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      onCreated(await createUserInvite({ username: username.trim(), display_name: displayName.trim() }));
    } catch (err) {
      setError(errorDetail(err, 'Nie udało się utworzyć użytkownika.'));
      setBusy(false);
    }
  };

  return (
    <StoreDialog
      title="Nowy użytkownik"
      onClose={onClose}
      footer={
        <>
          <button type="button" className="account-button" onClick={onClose}>
            Anuluj
          </button>
          <button
            type="submit"
            form={NEW_USER_FORM}
            className="account-button account-button--primary"
            disabled={busy || !username.trim() || !displayName.trim()}
          >
            Utwórz zaproszenie
          </button>
        </>
      }
    >
      <form id={NEW_USER_FORM} className="panel-stack" onSubmit={(event) => void handleSubmit(event)}>
        <label className="account-field-label" htmlFor="panel-new-username">
          Nazwa użytkownika
        </label>
        <input
          id="panel-new-username"
          className="form-control"
          value={username}
          onChange={(event) => setUsername(event.target.value)}
          autoFocus
          required
        />
        <label className="account-field-label" htmlFor="panel-new-display-name">
          Nazwa wyświetlana
        </label>
        <input
          id="panel-new-display-name"
          className="form-control"
          value={displayName}
          onChange={(event) => setDisplayName(event.target.value)}
          required
        />
        {error && (
          <div className="alert alert-danger mb-0" role="alert">
            {error}
          </div>
        )}
      </form>
    </StoreDialog>
  );
};

type ResetDialogProps = {
  user: UserAdmin;
  onClose: () => void;
  onDone: (invite: UserInviteResponse) => void;
};

const ResetDialog = ({ user, onClose, onDone }: ResetDialogProps) => {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleConfirm = async () => {
    setBusy(true);
    setError(null);
    try {
      onDone(await resetUserAccess(user.id));
    } catch (err) {
      setError(errorDetail(err, 'Nie udało się zresetować dostępu.'));
      setBusy(false);
    }
  };

  return (
    <StoreDialog
      title={`Zresetować dostęp: ${userLabel(user)}?`}
      onClose={onClose}
      footer={
        <>
          <button type="button" className="account-button" onClick={onClose}>
            Anuluj
          </button>
          <button
            type="button"
            className="account-button account-button--primary"
            disabled={busy}
            onClick={() => void handleConfirm()}
          >
            Zresetuj dostęp
          </button>
        </>
      }
    >
      <div className="panel-stack">
        <p className="panel-text">
          Usuniemy passkeys i aktywne sesje. Użytkownik wejdzie ponownie nowym kodem zaproszenia.
        </p>
        {error && (
          <div className="alert alert-danger mb-0" role="alert">
            {error}
          </div>
        )}
      </div>
    </StoreDialog>
  );
};

type DeleteDialogProps = {
  user: UserAdmin;
  onClose: () => void;
  onDone: () => void;
};

const DeleteDialog = ({ user, onClose, onDone }: DeleteDialogProps) => {
  const [confirmation, setConfirmation] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const label = userLabel(user);
  const confirmationRef = useRef<HTMLInputElement>(null);

  // Okno otwiera się z menu, które przy zamykaniu jeszcze trzyma fokus, więc `autoFocus` nie wystarcza.
  useEffect(() => {
    const frame = requestAnimationFrame(() => confirmationRef.current?.focus());
    return () => cancelAnimationFrame(frame);
  }, []);

  const handleConfirm = async () => {
    setBusy(true);
    setError(null);
    try {
      await deleteUser(user.id);
      onDone();
    } catch (err) {
      setError(errorDetail(err, 'Nie udało się usunąć konta.'));
      setBusy(false);
    }
  };

  return (
    <StoreDialog
      title={`Usunąć konto: ${label}?`}
      onClose={onClose}
      footer={
        <>
          <button type="button" className="account-button" onClick={onClose}>
            Anuluj
          </button>
          <button
            type="button"
            className="account-button account-button--danger-solid"
            disabled={busy || confirmation.trim() !== label.trim()}
            onClick={() => void handleConfirm()}
          >
            Usuń konto
          </button>
        </>
      }
    >
      <div className="panel-stack">
        <p className="panel-text">Usuniemy konto i wszystkie dane użytkownika. Tego nie da się cofnąć.</p>
        <label className="account-field-label" htmlFor="panel-delete-confirmation">
          Wpisz „{label}”, aby potwierdzić
        </label>
        <input
          id="panel-delete-confirmation"
          ref={confirmationRef}
          className="form-control"
          value={confirmation}
          onChange={(event) => setConfirmation(event.target.value)}
        />
        {error && (
          <div className="alert alert-danger mb-0" role="alert">
            {error}
          </div>
        )}
      </div>
    </StoreDialog>
  );
};

type UsersSectionProps = {
  currentUserId: string;
};

const UsersSection = ({ currentUserId }: UsersSectionProps) => {
  const [users, setUsers] = useState<UserAdmin[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [invite, setInvite] = useState<UserInviteResponse | null>(null);
  const [creating, setCreating] = useState(false);
  const [resetting, setResetting] = useState<UserAdmin | null>(null);
  const [deleting, setDeleting] = useState<UserAdmin | null>(null);

  const loadUsers = async (activeRef?: { current: boolean }) => {
    const isActive = () => (activeRef ? activeRef.current : true);
    setLoading(true);
    setError(null);
    try {
      const list = await getUsers();
      if (isActive()) setUsers(list);
    } catch (err) {
      if (isActive()) setError(errorDetail(err, 'Nie udało się pobrać użytkowników.'));
    } finally {
      if (isActive()) setLoading(false);
    }
  };

  useEffect(() => {
    const activeRef = { current: true };
    void loadUsers(activeRef);
    return () => {
      activeRef.current = false;
    };
  }, []);

  const handleCopy = async () => {
    if (!invite) return;
    try {
      await navigator.clipboard.writeText(invite.invite_token);
      toast('Skopiowano kod zaproszenia');
    } catch {
      toast.error('Nie udało się skopiować kodu.');
    }
  };

  const adminCount = users.filter((user) => user.is_admin).length;

  const deleteBlockedReason = (user: UserAdmin) => {
    if (user.id === currentUserId) return 'Nie można usunąć własnego konta.';
    if (user.is_admin && adminCount <= 1) return 'To ostatni administrator.';
    return null;
  };

  return (
    <section className="panel-section">
      <div className="panel-section__head">
        <h2 className="panel-section__title">
          Użytkownicy
          {users.length > 0 && <span className="panel-section__count">{users.length}</span>}
        </h2>
        <div className="panel-section__actions">
          <button
            className="account-button"
            type="button"
            onClick={() => void loadUsers()}
            disabled={loading}
          >
            <RefreshCw size={14} aria-hidden="true" />
            Odśwież
          </button>
          <button className="account-button account-button--primary" type="button" onClick={() => setCreating(true)}>
            <UserPlus size={14} aria-hidden="true" />
            Nowy użytkownik
          </button>
        </div>
      </div>

      {invite && (
        <div className="panel-invite">
          <div className="panel-invite__body">
            <span className="account-field-label">
              Kod zaproszenia dla: {invite.user.display_name || invite.user.username || 'użytkownika'}
            </span>
            <span className="panel-invite__token">{invite.invite_token}</span>
            <span className="account-hint">Ważny do: {formatDateTime(invite.invite_expires_at)}</span>
          </div>
          <div className="panel-section__actions">
            <button className="account-button" type="button" onClick={() => void handleCopy()}>
              <Copy size={14} aria-hidden="true" />
              Skopiuj
            </button>
            <button
              className="account-button panel-icon-button"
              type="button"
              aria-label="Zamknij kod zaproszenia"
              onClick={() => setInvite(null)}
            >
              <X size={14} aria-hidden="true" />
            </button>
          </div>
        </div>
      )}

      {error && (
        <div className="alert alert-danger mb-0" role="alert">
          {error}
        </div>
      )}

      <div className="panel-table-card">
        {loading && users.length === 0 ? (
          <div className="placeholder-glow panel-table-card__pad">
            <div className="placeholder col-6 mb-2" />
            <div className="placeholder col-8" />
          </div>
        ) : users.length === 0 ? (
          <p className="account-hint panel-table-card__pad">Brak użytkowników do wyświetlenia.</p>
        ) : (
          <div className="table-responsive">
            <ResizableTable tableId="users" className="table table-vcenter mb-0 panel-table">
              <thead>
                <tr>
                  <th>Użytkownik</th>
                  <th className="panel-hide-phone">Rola</th>
                  <th className="text-end panel-hide-phone">Passkeys</th>
                  <th className="panel-hide-phone">Utworzony</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {users.map((user) => {
                  const blockedReason = deleteBlockedReason(user);
                  return (
                    <tr key={user.id}>
                      <td>
                        <div className="panel-table__name">{userLabel(user)}</div>
                        {user.username && <div className="account-hint">@{user.username}</div>}
                      </td>
                      <td className="panel-hide-phone">{user.is_admin ? 'Administrator' : 'Użytkownik'}</td>
                      <td className="text-end panel-numbers panel-hide-phone">{user.passkey_count}</td>
                      <td className="panel-numbers panel-hide-phone">{formatDate(user.created_at)}</td>
                      <td className="panel-table__menu">
                        <DropdownMenu.Root>
                          <DropdownMenu.Trigger
                            className="accounts-menu-trigger"
                            aria-label={`Akcje: ${userLabel(user)}`}
                          >
                            <Ellipsis size={16} aria-hidden="true" />
                          </DropdownMenu.Trigger>
                          <DropdownMenu.Portal>
                            <DropdownMenu.Content
                              className="menu"
                              align="end"
                              sideOffset={4}
                              onCloseAutoFocus={(event) => event.preventDefault()}
                            >
                              <DropdownMenu.Item className="menu__item" onSelect={() => setResetting(user)}>
                                Zresetuj dostęp…
                              </DropdownMenu.Item>
                              <DropdownMenu.Separator className="menu__separator" />
                              <DropdownMenu.Item
                                className="menu__item panel-menu-danger"
                                disabled={Boolean(blockedReason)}
                                onSelect={() => setDeleting(user)}
                              >
                                <span>
                                  Usuń konto…
                                  {blockedReason && <span className="account-hint">{blockedReason}</span>}
                                </span>
                              </DropdownMenu.Item>
                            </DropdownMenu.Content>
                          </DropdownMenu.Portal>
                        </DropdownMenu.Root>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </ResizableTable>
          </div>
        )}
      </div>

      {creating && (
        <NewUserDialog
          onClose={() => setCreating(false)}
          onCreated={(created) => {
            setInvite(created);
            setCreating(false);
            toast('Utworzono zaproszenie');
            void loadUsers();
          }}
        />
      )}
      {resetting && (
        <ResetDialog
          user={resetting}
          onClose={() => setResetting(null)}
          onDone={(created) => {
            setInvite(created);
            setResetting(null);
            toast('Zresetowano dostęp');
            void loadUsers();
          }}
        />
      )}
      {deleting && (
        <DeleteDialog
          user={deleting}
          onClose={() => setDeleting(null)}
          onDone={() => {
            setDeleting(null);
            toast('Usunięto konto');
            void loadUsers();
          }}
        />
      )}
    </section>
  );
};

export default UsersSection;
