// Dolny pasek kart na telefonie: cztery najczęstsze ekrany i „Więcej", które otwiera boczne menu.
// Widoczność steruje CSS (tylko wąskie ekrany), więc komponent może stać w drzewie zawsze.
import { NavLink } from 'react-router-dom';
import { ArrowLeftRight, ChartColumn, Ellipsis, House, Wallet, type LucideIcon } from 'lucide-react';
import { copy } from '../copy/pl';

const labels = copy.nav;

const TABS: { to: string; label: string; icon: LucideIcon; end?: boolean }[] = [
  { to: '/', label: labels.home, icon: House, end: true },
  { to: '/budget', label: labels.budget, icon: Wallet },
  { to: '/transactions', label: labels.transactions, icon: ArrowLeftRight },
  { to: '/reports', label: labels.reports, icon: ChartColumn }
];

type MobileTabBarProps = {
  isMenuOpen: boolean;
  onOpenMenu: () => void;
};

const MobileTabBar = ({ isMenuOpen, onOpenMenu }: MobileTabBarProps) => (
  <nav className="mobile-tabbar" aria-label={labels.tabBar}>
    {TABS.map(({ to, label, icon: Icon, end }) => (
      <NavLink key={to} to={to} end={end} className="mobile-tabbar__item">
        <Icon size={22} aria-hidden="true" />
        {label}
      </NavLink>
    ))}
    <button
      type="button"
      className="mobile-tabbar__item"
      aria-expanded={isMenuOpen}
      aria-controls="app-sidebar"
      onClick={onOpenMenu}
    >
      <Ellipsis size={22} aria-hidden="true" />
      {labels.more}
    </button>
  </nav>
);

export default MobileTabBar;
