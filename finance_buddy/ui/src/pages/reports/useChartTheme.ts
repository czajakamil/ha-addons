import { useMemo } from 'react';
import { useTheme } from '../../hooks/useTheme';
import { readToken } from '../../utils/tokens';

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

/**
 * Kolory wykresów odczytane z tokenów motywu (tokens.css). Nivo nie rozumie
 * `var(--x)`, więc wartości trzeba pobrać z computed style; hook przelicza je
 * przy każdej zmianie `data-theme`, dzięki czemu wykresy podążają za trybem
 * jasnym/ciemnym.
 */
export const useChartTheme = (): ChartTheme => {
  const { theme } = useTheme();

  return useMemo(() => {
    void theme;

    const text = readToken('--text');
    const muted = readToken('--text-muted');
    const grid = readToken('--border');
    const surface = readToken('--surface');
    const menuBg = readToken('--surface');
    const fontFamily = readToken('--font-sans');

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
        grid: { line: { stroke: grid, strokeDasharray: '0' } },
        labels: { text: { fontFamily, fontWeight: 600 } },
        tooltip: { container: { background: menuBg, color: text, fontFamily, fontSize: 12, borderRadius: 6 } }
      }
    };
  }, [theme]);
};
