import { animated } from '@react-spring/web';
import type { Interpolation, SpringValue } from '@react-spring/web';
import { useTheme } from '@nivo/core';
import type { ComputedDatum } from '@nivo/pie';
import { moneyLabel, percentLabel, type BreakdownPieDatum } from './model';

// Kopia ArcLinkLabelProps z @nivo/arcs — pakiet eksportuje pod tą nazwą tylko
// domyślny komponent (wartość), więc typu nie da się stamtąd zaimportować.
type BreakdownArcLinkLabelProps = {
  datum: ComputedDatum<BreakdownPieDatum>;
  label: string;
  style: {
    path: Interpolation<string>;
    thickness: number;
    textPosition: Interpolation<string>;
    textAnchor: Interpolation<'start' | 'end'>;
    linkColor: SpringValue<string>;
    opacity: SpringValue<number>;
    textColor: SpringValue<string>;
  };
};

/**
 * Dwuwierszowa etykieta łącznikowa donuta w stylu YNAB: nazwa tagu, a pod nią
 * kwota z udziałem procentowym. Zastępuje domyślny (jednowierszowy) komponent
 * nivo, zachowując jego animowane style (path/pozycja/kolory).
 */
export const BreakdownArcLinkLabel = ({ datum, label, style }: BreakdownArcLinkLabelProps) => {
  const theme = useTheme();

  return (
    <animated.g opacity={style.opacity}>
      <animated.path
        fill="none"
        stroke={style.linkColor}
        strokeWidth={style.thickness}
        d={style.path}
      />
      <animated.text
        transform={style.textPosition}
        textAnchor={style.textAnchor}
        dominantBaseline="central"
        style={{ ...theme.labels.text, fill: style.textColor, fontSize: 12, fontWeight: 700 }}
      >
        <tspan x={0} dy="-0.4em">
          {label}
        </tspan>
        <tspan x={0} dy="1.4em" style={{ fontSize: 11, fontWeight: 500, opacity: 0.75 }}>
          {moneyLabel(datum.data.total)} ({percentLabel(datum.data.percentage)})
        </tspan>
      </animated.text>
    </animated.g>
  );
};
