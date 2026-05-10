import { useEffect, useState } from 'react';
import type { AuthUser } from './auth';
import { fetchMe, fetchSetupRequired, logout } from './auth';
import { Icon, type IconName } from './components/Icon';
import { loadAll, resetClientState, emitTargetsChanged } from './data';
import { fetchSettings, resetSettingsCache } from './agent/settings';
import { fetchUiPrefs, getUiPrefs, patchUiPrefs, resetUiPrefsCache, type UiPrefs } from './prefs';
import { AdminUsersScreen } from './screens/AdminUsersScreen';
import { ApiKeysScreen } from './screens/ApiKeysScreen';
import { SettingsScreen } from './screens/SettingsScreen';
import { AuthScreen } from './screens/AuthScreen';
import { ChangePasswordModal } from './screens/ChangePasswordModal';
import { ChatScreen } from './screens/ChatScreen';
import { PlanScreen } from './screens/PlanScreen';
import { RecipeDetail, RecipesScreen } from './screens/RecipesScreen';
import { ShoppingScreen } from './screens/ShoppingScreen';
import { TweakRadio, TweakSection, TweaksPanel, useTweaks } from './tweaks/TweaksPanel';
import type { MacroTarget, Tweaks } from './types';

const TWEAK_DEFAULTS: Tweaks = /*EDITMODE-BEGIN*/ {
  planLayout: 'grid',
  macroViz: 'progress',
  meals: ['Śniadanie', 'Obiad', 'Kolacja'],
} /*EDITMODE-END*/;

type Route = 'plan' | 'recipes' | 'shop' | 'chat' | 'users' | 'apikeys' | 'settings';
type AuthState =
  | { status: 'loading' }
  | { status: 'setup' }
  | { status: 'login' }
  | { status: 'authed'; user: AuthUser; dataReady: boolean };

interface NavItemProps {
  id: Route;
  icon: IconName;
  label: string;
  badge?: string;
  active: boolean;
  onSelect: (id: Route) => void;
}

function NavItem({ id, icon, label, badge, active, onSelect }: NavItemProps) {
  return (
    <button className={`nav-item ${active ? 'active' : ''}`} onClick={() => onSelect(id)}>
      <span className="ico">
        <Icon name={icon} size={17} />
      </span>
      <span>{label}</span>
      {badge && <span className="badge">{badge}</span>}
    </button>
  );
}

export function App() {
  const [auth, setAuth] = useState<AuthState>({ status: 'loading' });
  const [showChangePwd, setShowChangePwd] = useState(false);

  useEffect(() => {
    void (async () => {
      try {
        const me = await fetchMe();
        if (me) {
          await loadAll();
          await fetchSettings().catch(() => undefined);
          await fetchUiPrefs().catch(() => undefined);
          setAuth({ status: 'authed', user: me, dataReady: true });
          return;
        }
        const setupRequired = await fetchSetupRequired();
        setAuth({ status: setupRequired ? 'setup' : 'login' });
      } catch {
        setAuth({ status: 'login' });
      }
    })();
  }, []);

  async function onAuthenticated(user: AuthUser) {
    try {
      await loadAll();
      await fetchSettings().catch(() => undefined);
      await fetchUiPrefs().catch(() => undefined);
    } catch (e) {
      console.error('MealPilot: failed to load data', e);
    }
    setAuth({ status: 'authed', user, dataReady: true });
  }

  async function onLogout() {
    await logout();
    resetClientState();
    resetSettingsCache();
    resetUiPrefsCache();
    setAuth({ status: 'login' });
  }

  if (auth.status === 'loading') {
    return <div className="auth-screen">Ładowanie…</div>;
  }
  if (auth.status === 'setup' || auth.status === 'login') {
    return <AuthScreen mode={auth.status} onAuthenticated={onAuthenticated} />;
  }

  return <MainApp user={auth.user} onLogout={onLogout} onChangePassword={() => setShowChangePwd(true)} showChangePwd={showChangePwd} closeChangePwd={() => setShowChangePwd(false)} />;
}

interface MainAppProps {
  user: AuthUser;
  onLogout: () => void;
  onChangePassword: () => void;
  showChangePwd: boolean;
  closeChangePwd: () => void;
}

function MainApp({ user, onLogout, showChangePwd, closeChangePwd }: MainAppProps) {
  const [tweaks, setTweak] = useTweaks(TWEAK_DEFAULTS);
  const [uiPrefs, setUiPrefs] = useState<UiPrefs>(getUiPrefs);
  const [route, setRoute] = useState<Route>('plan');
  const [openId, setOpenId] = useState<string | null>(null);

  async function updateUiPref<K extends keyof UiPrefs>(key: K, value: UiPrefs[K]) {
    const next = { ...uiPrefs, [key]: value };
    setUiPrefs(next);
    await patchUiPrefs({ [key]: value });
  }

  async function onToggleFavorite(id: string) {
    const current = uiPrefs.favoriteRecipeIds ?? [];
    const next = current.includes(id) ? current.filter((x) => x !== id) : [...current, id];
    await updateUiPref('favoriteRecipeIds', next);
  }

  async function onTargetsChange(targets: MacroTarget) {
    await updateUiPref('macroTargets', targets);
    emitTargetsChanged();
  }

  const isAdmin = user.role === 'admin';

  return (
    <div className="app">
      <header className="mobile-topbar">
        <div className="brand">
          <div className="brand-mark">m</div>
          <div className="brand-name">
            Meal<em>Pilot</em>
          </div>
        </div>
        <button className="linklike mobile-logout" onClick={onLogout} aria-label="Wyloguj">
          {user.username} · Wyloguj
        </button>
      </header>
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">m</div>
          <div className="brand-name">
            Meal<em>Pilot</em>
          </div>
        </div>
        <nav className="nav">
          <NavItem
            id="plan"
            icon="calendar"
            label="Plan tygodnia"
            badge="15"
            active={route === 'plan'}
            onSelect={setRoute}
          />
          <NavItem
            id="recipes"
            icon="book"
            label="Przepisy"
            badge="8"
            active={route === 'recipes'}
            onSelect={setRoute}
          />
          <NavItem
            id="shop"
            icon="cart"
            label="Lista zakupów"
            active={route === 'shop'}
            onSelect={setRoute}
          />
          <NavItem
            id="chat"
            icon="bot"
            label="Asystent AI"
            active={route === 'chat'}
            onSelect={setRoute}
          />
          <NavItem
            id="settings"
            icon="cog"
            label="Ustawienia"
            active={route === 'settings'}
            onSelect={setRoute}
          />
          {isAdmin && (
            <NavItem
              id="users"
              icon="users"
              label="Użytkownicy"
              active={route === 'users'}
              onSelect={setRoute}
            />
          )}
          {isAdmin && (
            <NavItem
              id="apikeys"
              icon="key"
              label="Klucze API"
              active={route === 'apikeys'}
              onSelect={setRoute}
            />
          )}
        </nav>

        <div className="user-menu">
          <div style={{ flex: 1, minWidth: 0 }}>
            <div className="uname" title={user.username}>{user.username}</div>
            <div className="urole">{user.role}</div>
          </div>
          <button className="linklike" onClick={onLogout}>Wyloguj</button>
        </div>
      </aside>

      <main className={`main ${route === 'chat' ? 'main--chat' : ''}`}>
        {route === 'plan' && (
          <PlanScreen
            tweaks={tweaks}
            setTweak={setTweak}
            openRecipe={setOpenId}
            macroTargets={uiPrefs.macroTargets}
            onTargetsChange={onTargetsChange}
            favoriteIds={uiPrefs.favoriteRecipeIds ?? []}
          />
        )}
        {route === 'recipes' && (
          <RecipesScreen
            openRecipe={setOpenId}
            grouped={uiPrefs.recipesGrouped}
            onGroupedChange={(v) => void updateUiPref('recipesGrouped', v)}
            favoriteIds={uiPrefs.favoriteRecipeIds ?? []}
            onToggleFavorite={(id) => void onToggleFavorite(id)}
          />
        )}
        {route === 'shop' && <ShoppingScreen />}
        {route === 'chat' && <ChatScreen />}
        {route === 'users' && isAdmin && <AdminUsersScreen currentUserId={user.id} />}
        {route === 'apikeys' && isAdmin && <ApiKeysScreen />}
        {route === 'settings' && <SettingsScreen />}
      </main>

      {openId && (
        <RecipeDetail
          recipeId={openId}
          onClose={() => setOpenId(null)}
          isFavorite={(uiPrefs.favoriteRecipeIds ?? []).includes(openId)}
          onToggleFavorite={(id) => void onToggleFavorite(id)}
        />
      )}
      {showChangePwd && <ChangePasswordModal onClose={closeChangePwd} />}

      <TweaksPanel title="Tweaks">
        <TweakSection label="Plan tygodnia">
          <TweakRadio
            label="Layout"
            value={tweaks.planLayout}
            onChange={(v) => setTweak('planLayout', v)}
            options={[
              { value: 'grid', label: 'Grid' },
              { value: 'rows', label: 'Rzędy' },
              { value: 'compact', label: 'Kompakt' },
            ]}
          />
        </TweakSection>
        <TweakSection label="Wizualizacja makro">
          <TweakRadio
            label="Styl"
            value={tweaks.macroViz}
            onChange={(v) => setTweak('macroViz', v)}
            options={[
              { value: 'progress', label: 'Paski' },
              { value: 'donut', label: 'Donut' },
              { value: 'bar', label: 'Stack' },
            ]}
          />
        </TweakSection>
      </TweaksPanel>
    </div>
  );
}
