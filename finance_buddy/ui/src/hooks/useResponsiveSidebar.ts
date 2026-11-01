import { useEffect, useState } from 'react';

export const useResponsiveSidebar = (breakpoint = 1024) => {
  const collapsedStorageKey = `finance-buddy.sidebar-collapsed.${breakpoint}`;
  const [isMobileSidebarOpen, setMobileSidebarOpen] = useState(false);
  const [isDesktopSidebarCollapsed, setDesktopSidebarCollapsed] = useState(() => {
    if (typeof window === 'undefined') {
      return false;
    }
    return window.localStorage.getItem(collapsedStorageKey) === '1';
  });
  const [isDesktop, setIsDesktop] = useState(false);

  useEffect(() => {
    // zabezpieczenie pod ewentualne SSR / nietypowe środowiska
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
      return;
    }

    const mediaQuery = window.matchMedia(`(min-width: ${breakpoint}px)`);

    const applyMatch = (matches: boolean) => {
      setIsDesktop(matches);
      if (matches) {
        setMobileSidebarOpen(false);
      }
    };

    // ustaw stan na start
    applyMatch(mediaQuery.matches);

    // nie używamy MediaQueryListEvent, tylko zwykłego Event
    const handleMediaChange = (event: Event) => {
      const target = event.currentTarget as MediaQueryList;
      applyMatch(target.matches);
    };

    // nowoczesne API
    mediaQuery.addEventListener('change', handleMediaChange);

    return () => {
      mediaQuery.removeEventListener('change', handleMediaChange);
    };
  }, [breakpoint]);

  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }
    window.localStorage.setItem(collapsedStorageKey, isDesktopSidebarCollapsed ? '1' : '0');
  }, [collapsedStorageKey, isDesktopSidebarCollapsed]);

  const isSidebarOpen = isDesktop ? !isDesktopSidebarCollapsed : isMobileSidebarOpen;
  const isSidebarCollapsed = isDesktop && isDesktopSidebarCollapsed;

  const toggleSidebar = () => {
    if (isDesktop) {
      setDesktopSidebarCollapsed((prev) => !prev);
      return;
    }
    setMobileSidebarOpen((prev) => !prev);
  };

  const closeSidebar = () => {
    if (isDesktop) {
      setDesktopSidebarCollapsed(true);
      return;
    }
    setMobileSidebarOpen(false);
  };

  const openSidebar = () => {
    if (isDesktop) {
      setDesktopSidebarCollapsed(false);
      return;
    }
    setMobileSidebarOpen(true);
  };

  return {
    isDesktop,
    isSidebarOpen,
    isSidebarCollapsed,
    toggleSidebar,
    closeSidebar,
    openSidebar
  };
};
