import { useMemo } from 'react';
import { useTheme } from '../../hooks/useTheme';

export type ChartTheme = {
  /** Primary text color (chart center labels, emphasis). */
  text: string;
  /** Secondary text color (axis ticks, captions). */
  muted: string;
  /** Grid/axis line color. */
  grid: string;
  /** Card surface color (e.g. fills that must match the background). */
  surface: string;
  /** Ready-to-use theme object for Nivo charts. */
  nivoTheme: {
    axis: {
      domain: { line: { stroke: string } };
      ticks: { line: { stroke: string }; text: { fill: string; fontSize: number } };
    };
    grid: { line: { stroke: string; strokeDasharray: string } };
    labels: { text: { fontFamily: string; fontWeight: number } };
    tooltip: { container: { background: string; color: string; fontFamily: string } };
  };
};

const readToken = (styles: CSSStyleDeclaration, name: string, fallback: string) =>
  styles.getPropertyValue(name).trim() || fallback;

/**
 * Kolory wykresów odczytane z tokenów motywu (tokens.css). Nivo nie rozumie
 * `var(--x)`, więc wartości trzeba pobrać z computed style; hook przelicza je
 * przy każdej zmianie `data-theme`, dzięki czemu wykresy podążają za trybem
 * jasnym/ciemnym.
 */
export const useChartTheme = (): ChartTheme => {
  const { theme } = useTheme();

  return useMemo(() => {
    const styles =
      typeof window !== 'undefined'
        ? getComputedStyle(document.documentElement)
        : ({ getPropertyValue: () => '' } as unknown as CSSStyleDeclaration);
    void theme;

    const text = readToken(styles, '--text', '#15172a');
    const muted = readToken(styles, '--muted', '#6a7090');
    const grid = readToken(styles, '--border', '#e4e7f1');
    const surface = readToken(styles, '--surface', '#ffffff');
    const menuBg = readToken(styles, '--menu-bg', surface);
    const fontFamily = readToken(styles, '--font-sans', 'sans-serif');

    return {
      text,
      muted,
      grid,
      surface,
      nivoTheme: {
        axis: {
          domain: { line: { stroke: grid } },
          ticks: { line: { stroke: grid }, text: { fill: muted, fontSize: 12 } }
        },
        grid: { line: { stroke: grid, strokeDasharray: '4 6' } },
        labels: { text: { fontFamily, fontWeight: 600 } },
        tooltip: { container: { background: menuBg, color: text, fontFamily } }
      }
    };
  }, [theme]);
};
