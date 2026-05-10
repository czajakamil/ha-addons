import type { MacroTarget, MacroViz } from '../types';

interface MacroProps {
  kcal: number;
  p: number;
  f: number;
  c: number;
  variant?: MacroViz | 'row';
  target?: MacroTarget;
}

export function Macro({ kcal, p, f, c, variant = 'row', target }: MacroProps) {
  if (variant === 'row') {
    return (
      <div className="macro-row">
        <span className="seg" title="kcal">
          <span className="dot" style={{ background: 'var(--macro-k)' }} />
          {kcal}
          <span style={{ opacity: 0.55, marginLeft: 1 }}>kcal</span>
        </span>
        <span className="seg" title="białko">
          <span className="dot" style={{ background: 'var(--macro-p)' }} />
          {p}
          <span style={{ opacity: 0.55 }}>g</span>
        </span>
        <span className="seg" title="tłuszcz">
          <span className="dot" style={{ background: 'var(--macro-f)' }} />
          {f}
          <span style={{ opacity: 0.55 }}>g</span>
        </span>
        <span className="seg" title="węglowodany">
          <span className="dot" style={{ background: 'var(--macro-c)' }} />
          {c}
          <span style={{ opacity: 0.55 }}>g</span>
        </span>
      </div>
    );
  }
  if (variant === 'bar') {
    const total = p * 4 + f * 9 + c * 4 || 1;
    const pp = ((p * 4) / total) * 100;
    const fp = ((f * 9) / total) * 100;
    const cp = ((c * 4) / total) * 100;
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 4, minWidth: 0 }}>
        <div
          className="mono"
          style={{
            fontSize: 11,
            color: 'var(--ink-3)',
            display: 'flex',
            justifyContent: 'space-between',
          }}
        >
          <span>{kcal} kcal</span>
          <span style={{ opacity: 0.7 }}>
            B {p} · T {f} · W {c}
          </span>
        </div>
        <div
          style={{
            display: 'flex',
            height: 5,
            borderRadius: 99,
            overflow: 'hidden',
            background: 'var(--line-soft)',
          }}
        >
          <div style={{ width: `${pp}%`, background: 'var(--macro-p)' }} />
          <div style={{ width: `${fp}%`, background: 'var(--macro-f)' }} />
          <div style={{ width: `${cp}%`, background: 'var(--macro-c)' }} />
        </div>
      </div>
    );
  }
  if (variant === 'donut') {
    const total = p * 4 + f * 9 + c * 4 || 1;
    const r = 22;
    const C = 2 * Math.PI * r;
    const segs = [
      { v: (p * 4) / total, color: 'var(--macro-p)' },
      { v: (f * 9) / total, color: 'var(--macro-f)' },
      { v: (c * 4) / total, color: 'var(--macro-c)' },
    ];
    let off = 0;
    return (
      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
        <svg width={56} height={56} viewBox="0 0 56 56">
          <circle cx="28" cy="28" r={r} fill="none" stroke="var(--line-soft)" strokeWidth="6" />
          {segs.map((seg, i) => {
            const len = seg.v * C;
            const el = (
              <circle
                key={i}
                cx="28"
                cy="28"
                r={r}
                fill="none"
                stroke={seg.color}
                strokeWidth="6"
                strokeDasharray={`${len} ${C - len}`}
                strokeDashoffset={-off}
                transform="rotate(-90 28 28)"
                strokeLinecap="butt"
              />
            );
            off += len;
            return el;
          })}
          <text
            x="28"
            y="30"
            textAnchor="middle"
            fontFamily="var(--mono)"
            fontSize="10"
            fill="var(--ink-2)"
          >
            {kcal}
          </text>
        </svg>
        <div className="mono" style={{ fontSize: 11, lineHeight: 1.5, color: 'var(--ink-2)' }}>
          <div>
            <span
              className="dot"
              style={{
                display: 'inline-block',
                width: 6,
                height: 6,
                borderRadius: 99,
                background: 'var(--macro-p)',
                marginRight: 6,
              }}
            />
            B {p}g
          </div>
          <div>
            <span
              className="dot"
              style={{
                display: 'inline-block',
                width: 6,
                height: 6,
                borderRadius: 99,
                background: 'var(--macro-f)',
                marginRight: 6,
              }}
            />
            T {f}g
          </div>
          <div>
            <span
              className="dot"
              style={{
                display: 'inline-block',
                width: 6,
                height: 6,
                borderRadius: 99,
                background: 'var(--macro-c)',
                marginRight: 6,
              }}
            />
            W {c}g
          </div>
        </div>
      </div>
    );
  }
  if (variant === 'progress' && target) {
    const Bar = ({
      value,
      goal,
      color,
      label,
    }: {
      value: number;
      goal: number;
      color: string;
      label: string;
    }) => {
      const pct = Math.min(100, (value / goal) * 100);
      return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <div
            style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11 }}
            className="mono"
          >
            <span style={{ color: 'var(--ink-2)' }}>{label}</span>
            <span style={{ color: 'var(--ink-3)' }}>
              {value}
              <span style={{ opacity: 0.5 }}>/{goal}</span>
            </span>
          </div>
          <div
            style={{
              height: 5,
              borderRadius: 99,
              background: 'var(--line-soft)',
              overflow: 'hidden',
            }}
          >
            <div style={{ width: `${pct}%`, background: color, height: '100%' }} />
          </div>
        </div>
      );
    };
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <Bar value={kcal} goal={target.kcal} color="var(--macro-k)" label="kcal" />
        <Bar value={p} goal={target.p} color="var(--macro-p)" label="białko" />
        <Bar value={f} goal={target.f} color="var(--macro-f)" label="tłuszcz" />
        <Bar value={c} goal={target.c} color="var(--macro-c)" label="węglowodany" />
      </div>
    );
  }
  return null;
}
