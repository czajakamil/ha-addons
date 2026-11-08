// Tempo wydatków: bieżący miesiąc narastająco (linia ciągła) wobec średniej
// z poprzednich miesięcy (przerywana). Najechanie pokazuje oba stany na dany dzień.
import { useRef, useState, type PointerEvent } from 'react';
import type { HomePace } from '../../api';
import { formatSignedMoney } from '../../components/ui/Money';
import { copy } from '../../copy/pl';
import { HOME_CURRENCY, paceGeometry, paceVerdict } from './model';

const labels = copy.home;
const W = 320;
const H = 100;
const shortDay = new Intl.DateTimeFormat('pl-PL', {
  day: 'numeric',
  month: 'short'
});
const money = (cents: number) => formatSignedMoney(cents, HOME_CURRENCY);

type Props = { pace: HomePace; today: string };

const SpendingPace = ({ pace, today }: Props) => {
  const svgRef = useRef<SVGSVGElement>(null);
  const [hoverDay, setHoverDay] = useState<number | null>(null);
  const geometry = paceGeometry(pace, W, H);
  const [year, month] = today.split('-').map(Number);
  const dayLabel = (day: number) => shortDay.format(new Date(year, month - 1, day));
  const todayDay = pace.current_cents.length;
  const verdict = paceVerdict(pace);

  const onMove = (event: PointerEvent<SVGSVGElement>) => {
    const box = svgRef.current?.getBoundingClientRect();
    if (!box || box.width === 0) return;
    const ratio = Math.min(1, Math.max(0, (event.clientX - box.left) / box.width));
    setHoverDay(Math.round(ratio * (pace.days_in_month - 1)) + 1);
  };

  const at = (day: number, cents: number) => ({
    left: `${(geometry.x(day) / W) * 100}%`,
    top: `${(geometry.y(cents) / H) * 100}%`
  });

  const hovered = hoverDay ?? null;
  const hoverCurrent = hovered && hovered <= todayDay ? pace.current_cents[hovered - 1] : null;
  const hoverAverage = hovered && pace.average_cents.length ? pace.average_cents[hovered - 1] : null;

  const summary = [
    labels.paceCurrent(dayLabel(todayDay), money(pace.current_cents[todayDay - 1] ?? 0)),
    pace.average_cents.length ? labels.paceAverage(money(pace.average_cents[todayDay - 1])) : null
  ]
    .filter(Boolean)
    .join(', ');

  return (
    <section className="home-card home-pace" aria-labelledby="home-pace-title">
      <div className="home-card__head">
        <h2 id="home-pace-title" className="home-block__title">
          {labels.paceTitle}
        </h2>
        <span className="home-card__count">{labels.paceCumulative}</span>
      </div>

      <div className="home-pace__plot">
        <div className="home-pace__area">
          <svg
            ref={svgRef}
            viewBox={`0 0 ${W} ${H}`}
            preserveAspectRatio="none"
            role="img"
            aria-label={summary}
            className="home-pace__svg"
            onPointerMove={onMove}
            onPointerLeave={() => setHoverDay(null)}
          >
            <line x1={0} y1={H} x2={W} y2={H} className="home-pace__axis" />
            <line x1={0} y1={H / 2} x2={W} y2={H / 2} className="home-pace__grid" />
            <line x1={0} y1={0} x2={W} y2={0} className="home-pace__grid" />
            {geometry.average && <polyline points={geometry.average} className="home-pace__average" />}
            <line x1={geometry.x(todayDay)} y1={0} x2={geometry.x(todayDay)} y2={H} className="home-pace__today" />
            <polyline points={geometry.current} className="home-pace__current" />
            {hovered && (
              <line x1={geometry.x(hovered)} y1={0} x2={geometry.x(hovered)} y2={H} className="home-pace__crosshair" />
            )}
          </svg>
          {/* Kropki w HTML: SVG rozciąga się z szerokością karty, a koła nie mogą się spłaszczać. */}
          <span className="home-pace__point" style={at(todayDay, pace.current_cents[todayDay - 1] ?? 0)} />
          {hovered && hoverAverage !== null && (
            <span className="home-pace__point is-average" style={at(hovered, hoverAverage)} />
          )}
          {hovered && hoverCurrent !== null && <span className="home-pace__point" style={at(hovered, hoverCurrent)} />}
          {hovered && (
            <div
              className="home-pace__tooltip"
              style={{
                // Przy krawędziach dymek nie może wyjść poza kartę.
                left: `${Math.min(82, Math.max(18, (geometry.x(hovered) / W) * 100))}%`
              }}
              aria-hidden="true"
            >
              <span className="home-pace__tooltip-day">{dayLabel(hovered)}</span>
              {hoverCurrent !== null && (
                <span>
                  <span className="home-pace__swatch" /> {money(hoverCurrent)}
                </span>
              )}
              {hoverAverage !== null && (
                <span>
                  <span className="home-pace__swatch is-average" /> {money(hoverAverage)}
                </span>
              )}
            </div>
          )}
        </div>
        <div className="home-pace__labels" aria-hidden="true">
          <span>{dayLabel(1)}</span>
          {todayDay > 3 && todayDay < pace.days_in_month - 3 && (
            <span className="is-today" style={{ left: `${(geometry.x(todayDay) / W) * 100}%` }}>
              {labels.today}
            </span>
          )}
          <span>{dayLabel(pace.days_in_month)}</span>
        </div>
      </div>

      <div className="home-pace__legend">
        <span>
          <span className="home-pace__swatch" aria-hidden="true" />
          {labels.paceThisMonth} {money(pace.current_cents[todayDay - 1] ?? 0)}
        </span>
        {pace.average_cents.length > 0 && (
          <span>
            <span className="home-pace__swatch is-average" aria-hidden="true" />
            {labels.paceAverageLegend(pace.average_months)} {money(pace.average_cents[todayDay - 1])}
          </span>
        )}
      </div>
      {verdict && <p className="home-pace__verdict">{verdict}</p>}
    </section>
  );
};

export default SpendingPace;
