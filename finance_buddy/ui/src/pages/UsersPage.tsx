import { useApi } from '../hooks/useApi';
import { getSession, type AuthUser } from '../api';
import PageHeader from '../components/ui/PageHeader';
import BackupsCard from './panel/BackupsCard';
import UsersSection from './panel/UsersSection';
import YnabSection from './panel/YnabSection';

const UsersPage = () => {
  const { data: currentUser, loading } = useApi<AuthUser | null>(getSession, {
    errorMessage: 'Nie udało się pobrać sesji.'
  });

  if (loading) {
    return (
      <div className="page-shell panel-screen">
        <PageHeader title="Panel" />
        <div className="account-settings__card placeholder-glow">
          <div className="placeholder col-6 mb-2" />
          <div className="placeholder col-8" />
        </div>
      </div>
    );
  }

  if (!currentUser) {
    return (
      <div className="page-shell panel-screen">
        <PageHeader title="Panel" />
        <div className="alert alert-danger mb-0" role="alert">
          Brak sesji. Zaloguj się, aby zobaczyć panel.
        </div>
      </div>
    );
  }

  const isAdmin = Boolean(currentUser.is_admin);
  const name = currentUser.display_name || currentUser.username || 'Użytkownik';

  return (
    <div className="page-shell panel-screen">
      <PageHeader title="Panel" />

      <section className="panel-identity" aria-label="Konto">
        <span className="stores-mark stores-mark--large" aria-hidden="true">
          {name.charAt(0).toUpperCase()}
        </span>
        <div className="panel-identity__text">
          <span className="panel-table__name">{name}</span>
          <span className="account-hint">
            {currentUser.username ? `@${currentUser.username} · ` : ''}
            {isAdmin ? 'Administrator' : 'Użytkownik'}
          </span>
          {!isAdmin && (
            <span className="account-hint">Nowych użytkowników dodaje administrator.</span>
          )}
        </div>
      </section>

      <YnabSection />
      {isAdmin && <UsersSection currentUserId={currentUser.id} />}
      {isAdmin && <BackupsCard />}
    </div>
  );
};

export default UsersPage;
