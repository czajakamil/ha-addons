import { useEffect, useRef } from 'react';
import { REPORT_TABS, type ReportTabId } from '../model';

type ReportsTabBarProps = {
  activeTab: ReportTabId;
  onSelect: (tab: ReportTabId) => void;
};

const ReportsTabBar = ({ activeTab, onSelect }: ReportsTabBarProps) => {
  const navRef = useRef<HTMLElement>(null);

  // Na telefonie rząd przewija się w bok, więc aktywna zakładka musi sama wjechać do widoku
  useEffect(() => {
    const nav = navRef.current;
    const tab = nav?.querySelector<HTMLElement>('[aria-selected="true"]');
    if (!nav || !tab) return;
    nav.scrollTo({ left: tab.offsetLeft - (nav.clientWidth - tab.offsetWidth) / 2 });
  }, [activeTab]);

  return (
  <nav ref={navRef} className="reports-tabs reports-tabs--top" role="tablist" aria-label="Raporty finansowe">
    {REPORT_TABS.map((tab) => {
      const TabIcon = tab.icon;
      return (
        <button
          key={tab.id}
          type="button"
          role="tab"
          className={`reports-tab is-focusable${activeTab === tab.id ? ' is-active' : ''}`}
          aria-selected={activeTab === tab.id}
          onClick={() => onSelect(tab.id)}
        >
          <TabIcon className="reports-tab__icon" size={16} strokeWidth={1.8} aria-hidden="true" />
          <span className="reports-tab__label">{tab.label}</span>
        </button>
      );
    })}
  </nav>
  );
};

export default ReportsTabBar;
