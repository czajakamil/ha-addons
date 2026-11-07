// Pasek boczny: grupy nawigacji, zwijanie w nagłówku i menu użytkownika na dole.
import { NavLink } from 'react-router-dom';
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import {
  ArrowLeftRight,
  ChartColumn,
  ChevronsUpDown,
  CircleUser,
  FileUp,
  House,
  LogOut,
  Moon,
  Package,
  PanelLeftClose,
  PanelLeftOpen,
  PiggyBank,
  Receipt,
  Store,
  Sun,
  Tags,
  Wallet,
  type LucideIcon
} from 'lucide-react';
import type { AuthUser } from '../api';
import { copy } from '../copy/pl';
import type { ThemeMode } from '../hooks/useTheme';
import ReceiptsNavBadge from './ReceiptsNavBadge';
import UntaggedNavBadge from './UntaggedNavBadge';

export const navGroups: { label: string; links: { to: string; label: string; icon: LucideIcon }[] }[] = [
  {
    label: copy.nav.groups.daily,
    links: [
      { to: '/', label: copy.nav.home, icon: House },
      { to: '/budget', label: copy.nav.budget, icon: PiggyBank },
      { to: '/transactions', label: copy.nav.transactions, icon: ArrowLeftRight }
    ]
  },
  {
    label: copy.nav.groups.analysis,
    links: [{ to: '/reports', label: copy.nav.reports, icon: ChartColumn }]
  },
  {
    label: copy.nav.groups.data,
    links: [
      { to: '/accounts', label: copy.nav.accounts, icon: Wallet },
      { to: '/stores', label: copy.nav.stores, icon: Store },
      { to: '/receipts', label: copy.nav.receipts, icon: Receipt },
      { to: '/bank-import', label: copy.nav.bankImport, icon: FileUp },
      { to: '/items', label: copy.nav.items, icon: Package },
      { to: '/categories', label: copy.nav.tags, icon: Tags }
    ]
  }
];

type AppSidebarProps = {
  user: AuthUser;
  theme: ThemeMode;
  isOpen: boolean;
  isCollapsed: boolean;
  isDesktop: boolean;
  onToggle: () => void;
  onToggleTheme: () => void;
  onNavigate: () => void;
  onOpenPanel: () => void;
  onLogout: () => void;
};

const AppSidebar = ({
  user,
  theme,
  isOpen,
  isCollapsed,
  isDesktop,
  onToggle,
  onToggleTheme,
  onNavigate,
  onOpenPanel,
  onLogout
}: AppSidebarProps) => {
  const classes = ['app__sidebar', isOpen ? 'is-open' : '', isCollapsed ? 'is-collapsed' : '']
    .filter(Boolean)
    .join(' ');
  const userName = user.display_name || user.username || copy.user.fallbackName;
  const collapseLabel = isCollapsed ? copy.nav.expand : copy.nav.collapse;
  const CollapseIcon = isCollapsed ? PanelLeftOpen : PanelLeftClose;

  return (
    <aside id="app-sidebar" className={classes}>
      <div className="sidebar__header">
        <span className="sidebar__brand">Budget Buddy</span>
        {isDesktop && (
          <button
            type="button"
            className="sidebar__icon-button"
            aria-label={collapseLabel}
            title={collapseLabel}
            aria-expanded={!isCollapsed}
            aria-controls="app-sidebar"
            onClick={onToggle}
          >
            <CollapseIcon size={16} aria-hidden="true" />
          </button>
        )}
      </div>

      <nav className="sidebar__nav" aria-label={copy.nav.mainNavigation}>
        {navGroups.map((group) => (
          <div key={group.label} className="sidebar__group">
            <p className="sidebar__group-label">{group.label}</p>
            {group.links.map(({ to, label, icon: LinkIcon }) => (
              <NavLink
                key={to}
                to={to}
                end={to === '/'}
                className={({ isActive }) => `sidebar__link${isActive ? ' is-active' : ''}`}
                title={isCollapsed ? label : undefined}
                onClick={onNavigate}
              >
                <LinkIcon className="sidebar__icon" size={16} aria-hidden="true" />
                <span className="sidebar__text">{label}</span>
                {/* Licznik „do otagowania" przy Transakcjach — kolejka jest ich podstroną. */}
                {to === '/transactions' ? <UntaggedNavBadge /> : null}
                {to === '/receipts' ? <ReceiptsNavBadge /> : null}
              </NavLink>
            ))}
          </div>
        ))}
      </nav>

      <div className="sidebar__footer">
        <DropdownMenu.Root>
          <DropdownMenu.Trigger className="sidebar__user" aria-label={copy.user.menu} title={isCollapsed ? userName : undefined}>
            <CircleUser className="sidebar__icon" size={16} aria-hidden="true" />
            <span className="sidebar__text">{userName}</span>
            <ChevronsUpDown className="sidebar__user-chevron" size={14} aria-hidden="true" />
          </DropdownMenu.Trigger>
          <DropdownMenu.Portal>
            <DropdownMenu.Content className="menu" side="top" align="start" sideOffset={4}>
              <DropdownMenu.Item className="menu__item" onSelect={onOpenPanel}>
                <CircleUser size={16} aria-hidden="true" />
                {user.is_admin ? copy.user.adminPanel : copy.user.userPanel}
              </DropdownMenu.Item>
              <DropdownMenu.Item
                className="menu__item"
                onSelect={(event) => {
                  event.preventDefault();
                  onToggleTheme();
                }}
              >
                {theme === 'dark' ? <Moon size={16} aria-hidden="true" /> : <Sun size={16} aria-hidden="true" />}
                {theme === 'dark' ? copy.user.themeDark : copy.user.themeLight}
              </DropdownMenu.Item>
              <DropdownMenu.Separator className="menu__separator" />
              <DropdownMenu.Item className="menu__item" onSelect={onLogout}>
                <LogOut size={16} aria-hidden="true" />
                {copy.user.logout}
              </DropdownMenu.Item>
            </DropdownMenu.Content>
          </DropdownMenu.Portal>
        </DropdownMenu.Root>
      </div>
    </aside>
  );
};

export default AppSidebar;
