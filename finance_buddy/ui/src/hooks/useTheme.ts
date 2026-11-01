import { useCallback, useEffect, useState } from 'react';

export type ThemeMode = 'light' | 'dark';

const STORAGE_KEY = 'fb-theme';

const readStoredTheme = (): ThemeMode => {
  if (typeof window === 'undefined') return 'light';
  try {
    return window.localStorage.getItem(STORAGE_KEY) === 'dark' ? 'dark' : 'light';
  } catch {
    return 'light';
  }
};

const applyTheme = (theme: ThemeMode) => {
  if (typeof document === 'undefined') return;
  document.documentElement.dataset.theme = theme;
  // Keep Tabler/Bootstrap component dark-mode tweaks in sync.
  document.documentElement.dataset.bsTheme = theme;
};

/**
 * Light/dark theme controlled via `data-theme` on <html> and persisted to
 * localStorage. The initial value is applied by an inline script in index.html
 * to avoid a flash; this hook keeps React in sync and lets components toggle it.
 */
export const useTheme = () => {
  const [theme, setThemeState] = useState<ThemeMode>(readStoredTheme);

  useEffect(() => {
    applyTheme(theme);
    try {
      window.localStorage.setItem(STORAGE_KEY, theme);
    } catch {
      /* ignore persistence errors (e.g. private mode) */
    }
  }, [theme]);

  // useTheme has no shared context, so each instance keeps its own state. Watch
  // the `data-theme` attribute on <html> so every instance (e.g. the sidebar
  // toggle and a DataTable deep in the tree) stays in sync when any of them
  // flips the theme.
  useEffect(() => {
    if (typeof document === 'undefined') return;
    const root = document.documentElement;
    const observer = new MutationObserver(() => {
      const next: ThemeMode = root.dataset.theme === 'dark' ? 'dark' : 'light';
      setThemeState((prev) => (prev === next ? prev : next));
    });
    observer.observe(root, { attributes: true, attributeFilter: ['data-theme'] });
    return () => observer.disconnect();
  }, []);

  const setTheme = useCallback((next: ThemeMode) => setThemeState(next), []);
  const toggleTheme = useCallback(
    () => setThemeState((prev) => (prev === 'dark' ? 'light' : 'dark')),
    []
  );

  return { theme, setTheme, toggleTheme };
};
