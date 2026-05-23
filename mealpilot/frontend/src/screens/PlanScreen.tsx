import { Fragment, useEffect, useMemo, useRef, useState } from 'react';
import { Icon } from '../components/Icon';
import { Macro } from '../components/Macro';
import { RecipeThumb } from '../components/RecipeThumb';
import {
  DAYS,
  MEAL_TYPES_ALL,
  PLAN_CHANGED,
  RECIPES_CHANGED,
  currentWeekStart,
  getPlan,
  getRecipes,
  recipeBy,
  listTemplates,
  createTemplate,
  deleteTemplate,
  applyTemplate,
  emitPlanChanged,
  loadPlan,
} from '../data';
import type { MacroTarget, PlanEntry, SetTweak, Tweaks, WeekTemplate } from '../types';

interface Props {
  tweaks: Tweaks;
  setTweak: SetTweak;
  openRecipe: (id: string) => void;
  macroTargets: MacroTarget;
  onTargetsChange: (t: MacroTarget) => Promise<void>;
  favoriteIds: string[];
}

interface DragState {
  fromKey: string;
  recipe_id: string;
  servings: number;
}

export function PlanScreen({ tweaks, setTweak, openRecipe, macroTargets, onTargetsChange, favoriteIds }: Props) {
  const [weekStart, setWeekStart] = useState(currentWeekStart);
  const [plan, setPlan] = useState<PlanEntry[]>(() => [...getPlan(currentWeekStart())]);

  useEffect(() => {
    const onPlanChanged = () => setPlan([...getPlan(weekStart)]);
    const onRecipesChanged = () => setPlan((prev) => [...prev]);
    window.addEventListener(PLAN_CHANGED, onPlanChanged);
    window.addEventListener(RECIPES_CHANGED, onRecipesChanged);
    return () => {
      window.removeEventListener(PLAN_CHANGED, onPlanChanged);
      window.removeEventListener(RECIPES_CHANGED, onRecipesChanged);
    };
  }, [weekStart]);
  const [over, setOver] = useState<{ day: number; meal: string } | null>(null);
  const [picker, setPicker] = useState<{ day: number; meal: string } | null>(null);
  const [pickerQ, setPickerQ] = useState('');
  const [showMealConfig, setShowMealConfig] = useState(false);
  const [showTemplates, setShowTemplates] = useState(false);
  const [templates, setTemplates] = useState<WeekTemplate[]>([]);
  const [saveTemplateName, setSaveTemplateName] = useState('');
  const [savingTemplate, setSavingTemplate] = useState(false);
  const [applyingId, setApplyingId] = useState<number | null>(null);
  const [showGoals, setShowGoals] = useState(false);
  const [goalsForm, setGoalsForm] = useState<MacroTarget>(macroTargets);
  const [goalsSaving, setGoalsSaving] = useState(false);
  const [expandedDayMacros, setExpandedDayMacros] = useState(false);
  const dragRef = useRef<DragState | null>(null);

  const todayIndex = useMemo(() => {
    const today = new Date();
    const start = new Date(weekStart);
    const diff = Math.round((today.getTime() - start.getTime()) / 86400000);
    return diff >= 0 && diff < 7 ? diff : -1;
  }, [weekStart]);

  useEffect(() => {
    setGoalsForm(macroTargets);
  }, [macroTargets]);

  const [isNarrow, setIsNarrow] = useState(() =>
    typeof window !== 'undefined' && window.matchMedia('(max-width: 720px)').matches,
  );
  useEffect(() => {
    if (typeof window === 'undefined') return;
    const mql = window.matchMedia('(max-width: 720px)');
    const onChange = (e: MediaQueryListEvent) => setIsNarrow(e.matches);
    mql.addEventListener('change', onChange);
    return () => mql.removeEventListener('change', onChange);
  }, []);
  const layout = isNarrow ? 'rows' : tweaks.planLayout || 'grid';
  const enabledMeals = tweaks.meals || ['Śniadanie', 'Obiad', 'Kolacja'];

  const cellEntries = (day: number, meal: string) =>
    plan.filter((p) => p.day === day && p.meal.toLowerCase() === meal.toLowerCase());

  const moveTo = (entry: { recipe_id: string; servings: number }, fromKey: string, day: number, meal: string) => {
    setPlan((prev) => {
      let next = prev;
      if (fromKey && fromKey !== 'library') {
        const [fd, fm, fr] = fromKey.split('|');
        next = next.filter((p) => !(p.day === +fd && p.meal === fm && p.recipe_id === fr));
      }
      next = next.filter((p) => !(p.day === day && p.meal === meal && p.recipe_id === entry.recipe_id));
      return [...next, { day, meal, recipe_id: entry.recipe_id, servings: entry.servings || 1 }];
    });
  };

  const removeEntry = (day: number, meal: string, recipe: string) =>
    setPlan((prev) => prev.filter((p) => !(p.day === day && p.meal === meal && p.recipe_id === recipe)));

  const addRecipe = (day: number, meal: string, recipeId: string) => {
    setPlan((prev) => {
      const filtered = prev.filter(
        (p) => !(p.day === day && p.meal === meal && p.recipe_id === recipeId),
      );
      return [...filtered, { day, meal, recipe_id: recipeId, servings: 1 }];
    });
    setPicker(null);
    setPickerQ('');
  };

  const totals = useMemo(() => {
    let kcal = 0,
      p = 0,
      f = 0,
      c = 0;
    plan.forEach((e) => {
      const r = recipeBy(e.recipe_id);
      if (!r) return;
      const portions = e.servings || 1;
      kcal += (r.kcal / r.servings) * portions;
      p += (r.p / r.servings) * portions;
      f += (r.f / r.servings) * portions;
      c += (r.c / r.servings) * portions;
    });
    return {
      kcal: Math.round(kcal),
      p: Math.round(p),
      f: Math.round(f),
      c: Math.round(c),
      meals: plan.length,
      days: new Set(plan.map((p) => p.day)).size,
    };
  }, [plan]);

  const dayTotals = (day: number) => {
    let kcal = 0,
      p = 0,
      f = 0,
      c = 0;
    plan
      .filter((e) => e.day === day)
      .forEach((e) => {
        const r = recipeBy(e.recipe_id);
        if (!r) return;
        const portions = e.servings || 1;
        kcal += (r.kcal / r.servings) * portions;
        p += (r.p / r.servings) * portions;
        f += (r.f / r.servings) * portions;
        c += (r.c / r.servings) * portions;
      });
    return { kcal: Math.round(kcal), p: Math.round(p), f: Math.round(f), c: Math.round(c) };
  };

  const fmtWeek = (s: string) => {
    const start = new Date(s);
    const end = new Date(start.getTime() + 6 * 86400000);
    const fmt = (d: Date) =>
      `${d.getDate()} ${['sty', 'lut', 'mar', 'kwi', 'maj', 'cze', 'lip', 'sie', 'wrz', 'paź', 'lis', 'gru'][d.getMonth()]}`;
    return `${fmt(start)} – ${fmt(end)} ${end.getFullYear()}`;
  };

  const shiftWeek = (n: number) => {
    const d = new Date(weekStart);
    d.setDate(d.getDate() + n * 7);
    const ws = d.toISOString().slice(0, 10);
    setWeekStart(ws);
    loadPlan(ws).then(() => setPlan([...getPlan(ws)]));
  };

  const toggleMeal = (id: string) => {
    const cur = enabledMeals;
    const next = cur.includes(id)
      ? cur.filter((m) => m !== id)
      : MEAL_TYPES_ALL.filter((m) => cur.includes(m.id) || m.id === id).map((m) => m.id);
    setTweak('meals', next);
    if (cur.includes(id)) {
      setPlan((prev) => prev.filter((p) => p.meal !== id));
    }
  };

  const Cell = ({ day, meal, compact }: { day: number; meal: string; compact: boolean }) => {
    const entries = cellEntries(day, meal);
    const isOver = !!over && over.day === day && over.meal === meal;
    return (
      <div
        className="plan-cell"
        data-empty={entries.length === 0}
        data-over={isOver}
        onDragOver={(e) => {
          e.preventDefault();
          e.dataTransfer.dropEffect = 'move';
          if (!isOver) setOver({ day, meal });
        }}
        onDragLeave={(e) => {
          if (e.currentTarget.contains(e.relatedTarget as Node)) return;
          setOver(null);
        }}
        onDrop={(e) => {
          e.preventDefault();
          const d = dragRef.current;
          if (!d) return;
          moveTo({ recipe_id: d.recipe_id, servings: d.servings || 1 }, d.fromKey, day, meal);
          dragRef.current = null;
          setOver(null);
        }}
      >
        {entries.length === 0 ? (
          <button className="cell-add" onClick={() => setPicker({ day, meal })}>
            <Icon name="plus" size={14} />
            <span>dodaj</span>
          </button>
        ) : (
          entries.map((e, i) => {
            const r = recipeBy(e.recipe_id);
            if (!r) return null;
            return (
              <div
                key={i}
                className="meal-card"
                draggable={true}
                onDragStart={(ev) => {
                  dragRef.current = {
                    fromKey: `${day}|${meal}|${e.recipe_id}`,
                    recipe_id: e.recipe_id,
                    servings: e.servings,
                  };
                  ev.dataTransfer.effectAllowed = 'move';
                  try {
                    ev.dataTransfer.setData('text/plain', e.recipe_id);
                  } catch {
                    /* noop */
                  }
                }}
                onDragEnd={() => {
                  dragRef.current = null;
                  setOver(null);
                }}
                onClick={() => openRecipe(r.id)}
                style={{ ['--hue' as string]: r.hue } as React.CSSProperties}
              >
                <div
                  className="meal-card-stripe"
                  style={{ background: `oklch(0.65 0.13 ${r.hue})` }}
                />
                <div className="meal-card-body">
                  <div className="meal-card-title">{r.title}</div>
                  {!compact && (
                    <div className="meal-card-meta">
                      <span className="mono">{Math.round(r.kcal / r.servings)} kcal</span>
                      <span style={{ opacity: 0.4 }}>·</span>
                      <span className="mono">{r.prep_time + r.cook_time}min</span>
                    </div>
                  )}
                </div>
                <button
                  className="meal-card-x"
                  onClick={(ev) => {
                    ev.stopPropagation();
                    removeEntry(day, meal, e.recipe_id);
                  }}
                  aria-label="usuń"
                >
                  <Icon name="x" size={12} />
                </button>
              </div>
            );
          })
        )}
        {entries.length > 0 && (
          <button className="cell-add cell-add-mini" onClick={() => setPicker({ day, meal })}>
            <Icon name="plus" size={11} />
          </button>
        )}
      </div>
    );
  };

  const dayHeader = (i: number) => {
    const d = new Date(new Date(weekStart).getTime() + i * 86400000);
    const t = dayTotals(i);
    const kcalPct = macroTargets.kcal > 0 ? Math.min(100, (t.kcal / macroTargets.kcal) * 100) : 0;
    const kcalOver = t.kcal > macroTargets.kcal;

    const MiniBar = ({ value, goal, color }: { value: number; goal: number; color: string }) => (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between' }} className="mono">
          <span style={{ fontSize: 9, color: 'var(--ink-faint)' }}>{value}g</span>
          <span style={{ fontSize: 9, opacity: 0.4 }}>/{goal}</span>
        </div>
        <div style={{ height: 3, borderRadius: 99, background: 'var(--line-soft)', overflow: 'hidden' }}>
          <div style={{ width: `${Math.min(100, (value / goal) * 100)}%`, height: '100%', background: color }} />
        </div>
      </div>
    );

    return (
      <div className="plan-day-head">
        <div className="plan-day-name">{DAYS[i]}</div>
        <div className="plan-day-date mono">
          {d.getDate()}/{d.getMonth() + 1}
        </div>
        {t.kcal > 0 && (
          <div style={{ width: '100%', marginTop: 4 }}>
            <div className="mono" style={{ fontSize: 10, color: kcalOver ? 'var(--terra, #b34)' : 'var(--ink-faint)', marginBottom: 2 }}>
              {t.kcal}<span style={{ opacity: 0.5 }}>/{macroTargets.kcal}</span>
            </div>
            <div style={{ height: 3, borderRadius: 99, background: 'var(--line-soft)', overflow: 'hidden' }}>
              <div style={{ width: `${kcalPct}%`, height: '100%', background: kcalOver ? 'var(--terra, #b34)' : 'var(--macro-k)', transition: 'width .3s' }} />
            </div>
            {expandedDayMacros && (
              <div style={{ marginTop: 6, display: 'flex', flexDirection: 'column', gap: 4 }}>
                <MiniBar value={t.p} goal={macroTargets.p} color="var(--macro-p)" />
                <MiniBar value={t.f} goal={macroTargets.f} color="var(--macro-f)" />
                <MiniBar value={t.c} goal={macroTargets.c} color="var(--macro-c)" />
              </div>
            )}
          </div>
        )}
      </div>
    );
  };

  const WeekDotStrip = () => {
    const R = 14;
    const circ = 2 * Math.PI * R;
    return (
      <div className="week-dots">
        {DAYS.map((d, i) => {
          const count = plan.filter((p) => p.day === i).length;
          const pct = enabledMeals.length > 0 ? Math.min(1, count / enabledMeals.length) : 0;
          const isToday = i === todayIndex;
          return (
            <div key={d} className={`week-dot-item${isToday ? ' week-dot-today' : ''}`}>
              <svg width={34} height={34} viewBox="0 0 34 34">
                <circle cx={17} cy={17} r={R} fill={isToday ? 'var(--accent-soft)' : 'none'} stroke="var(--line-soft)" strokeWidth={2} />
                {count > 0 && (
                  <circle
                    cx={17} cy={17} r={R}
                    fill="none"
                    stroke={isToday ? 'var(--accent)' : 'var(--ink-3)'}
                    strokeWidth={2.5}
                    strokeDasharray={`${pct * circ} ${circ}`}
                    strokeLinecap="round"
                    transform="rotate(-90 17 17)"
                    style={{ transition: 'stroke-dasharray .3s' }}
                  />
                )}
                <text
                  x={17} y={17}
                  textAnchor="middle"
                  dominantBaseline="central"
                  fontSize={9}
                  fontFamily="var(--mono)"
                  fill={isToday ? 'var(--accent-deep)' : 'var(--ink-2)'}
                  fontWeight={isToday ? 600 : 400}
                >
                  {d}
                </text>
              </svg>
              <div className="week-dot-count mono">{count > 0 ? `${count}/${enabledMeals.length}` : ''}</div>
            </div>
          );
        })}
      </div>
    );
  };

  const renderGrid = (compact = false) => (
    <div
      className={`plan-grid ${compact ? 'plan-grid-compact' : ''}`}
      style={{
        gridTemplateRows: `auto repeat(${enabledMeals.length}, minmax(${compact ? 56 : 92}px, auto))`,
      }}
    >
      <div className="plan-grid-corner">
        <button
          onClick={() => setExpandedDayMacros((v) => !v)}
          style={{ all: 'unset', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', width: '100%', height: '100%', color: 'var(--ink-faint)' }}
          title={expandedDayMacros ? 'Zwiń makro' : 'Rozwiń makro'}
        >
          <svg width={13} height={13} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round" style={{ transform: expandedDayMacros ? 'rotate(180deg)' : 'none', transition: 'transform .2s' }}>
            <path d="M6 9l6 6 6-6" />
          </svg>
        </button>
      </div>
      {DAYS.map((_, i) => (
        <Fragment key={i}>{dayHeader(i)}</Fragment>
      ))}
      {enabledMeals.map((meal) => (
        <Fragment key={meal}>
          <div className="plan-meal-head">
            <span
              className="serif"
              style={{ fontSize: compact ? 14 : 15, fontStyle: 'italic' }}
            >
              {meal}
            </span>
          </div>
          {DAYS.map((_, day) => (
            <Cell key={`${meal}-${day}`} day={day} meal={meal} compact={compact} />
          ))}
        </Fragment>
      ))}
    </div>
  );

  const renderRows = () => (
    <div className="plan-rows">
      {DAYS.map((d, day) => {
        const t = dayTotals(day);
        return (
          <div key={d} className="plan-row card paper-grain">
            <div className="plan-row-head">
              <div>
                <div className="serif" style={{ fontSize: 20, fontStyle: 'italic' }}>
                  {d}
                </div>
                <div className="mono" style={{ fontSize: 11, color: 'var(--ink-faint)' }}>
                  {new Date(new Date(weekStart).getTime() + day * 86400000).getDate()}/
                  {new Date(new Date(weekStart).getTime() + day * 86400000).getMonth() + 1}
                </div>
              </div>
              {t.kcal > 0 && (
                <div style={{ textAlign: 'right', minWidth: 220 }}>
                  <Macro {...t} variant="progress" target={macroTargets} />
                </div>
              )}
            </div>
            <div
              className="plan-row-cells"
              style={isNarrow ? undefined : { gridTemplateColumns: `repeat(${enabledMeals.length}, 1fr)` }}
            >
              {enabledMeals.map((meal) => (
                <div key={meal} className="plan-row-meal">
                  <div className="plan-row-meal-label">{meal}</div>
                  <Cell day={day} meal={meal} compact={isNarrow} />
                </div>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );

  const openTemplates = () => {
    setShowTemplates(true);
    setSaveTemplateName('');
    void listTemplates().then(setTemplates);
  };

  const handleSaveTemplate = async () => {
    if (!saveTemplateName.trim()) return;
    setSavingTemplate(true);
    try {
      const tpl = await createTemplate(saveTemplateName.trim(), plan);
      setTemplates((prev) => [tpl, ...prev]);
      setSaveTemplateName('');
    } finally {
      setSavingTemplate(false);
    }
  };

  const handleApplyTemplate = async (id: number) => {
    setApplyingId(id);
    try {
      const entries = await applyTemplate(id, weekStart);
      setPlan([...entries]);
      emitPlanChanged();
      setShowTemplates(false);
    } finally {
      setApplyingId(null);
    }
  };

  const handleDeleteTemplate = async (id: number) => {
    await deleteTemplate(id);
    setTemplates((prev) => prev.filter((t) => t.id !== id));
  };

  const handleSaveGoals = async () => {
    setGoalsSaving(true);
    try {
      await onTargetsChange(goalsForm);
      setShowGoals(false);
    } finally {
      setGoalsSaving(false);
    }
  };

  const recipes = getRecipes();
  const libraryRecipes = favoriteIds.length > 0
    ? recipes.filter((r) => favoriteIds.includes(r.id))
    : recipes;
  const pickerList = recipes.filter(
    (r) =>
      pickerQ === '' ||
      r.title.toLowerCase().includes(pickerQ.toLowerCase()) ||
      r.tags.some((t) => t.toLowerCase().includes(pickerQ.toLowerCase())),
  );

  return (
    <div>
      <div className="page-head">
        <div>
          <div className="eyebrow">Tydzień</div>
          <h1 className="serif" style={{ fontStyle: 'italic' }}>
            Plan tygodnia
          </h1>
          <div className="sub">
            {fmtWeek(weekStart)} · {totals.meals} posiłków · {totals.days}/7 dni
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
          <div
            style={{
              display: 'flex',
              gap: 0,
              border: '1px solid var(--line)',
              borderRadius: 'var(--r)',
              overflow: 'hidden',
              background: 'var(--card)',
            }}
          >
            <button
              className="btn ghost"
              style={{ borderRadius: 0, boxShadow: 'none' }}
              onClick={() => shiftWeek(-1)}
            >
              <Icon name="chev-l" size={14} />
            </button>
            <button
              className="btn ghost"
              style={{
                borderRadius: 0,
                boxShadow: 'none',
                borderLeft: '1px solid var(--line-soft)',
                borderRight: '1px solid var(--line-soft)',
                fontSize: 12,
              }}
              onClick={() => { const ws = currentWeekStart(); setWeekStart(ws); loadPlan(ws).then(() => setPlan([...getPlan(ws)])); }}
            >
              Bieżący
            </button>
            <button
              className="btn ghost"
              style={{ borderRadius: 0, boxShadow: 'none' }}
              onClick={() => shiftWeek(1)}
            >
              <Icon name="chev-r" size={14} />
            </button>
          </div>
          <button className="btn" onClick={() => { setGoalsForm(macroTargets); setShowGoals(true); }}>
            <Icon name="target" size={14} /> Cele
          </button>
          <button className="btn" onClick={openTemplates}>
            <Icon name="star" size={14} /> Szablony
          </button>
          <button className="btn" onClick={() => setShowMealConfig(true)}>
            Posiłki{' '}
            <span
              className="mono"
              style={{ fontSize: 11, color: 'var(--ink-faint)', marginLeft: 4 }}
            >
              {enabledMeals.length}
            </span>
          </button>
        </div>
      </div>

      <div className="week-strip">
        <div className="week-strip-num">
          <div
            className="mono"
            style={{
              fontSize: 10,
              letterSpacing: '.1em',
              color: 'var(--ink-faint)',
              textTransform: 'uppercase',
            }}
          >
            średnio dziennie
          </div>
          <div
            className="serif"
            style={{ fontSize: 30, fontStyle: 'italic', letterSpacing: '-0.02em' }}
          >
            {Math.round(totals.kcal / Math.max(1, totals.days))}{' '}
            <span style={{ fontSize: 14, opacity: 0.55 }}>kcal</span>
          </div>
        </div>
        <div style={{ flex: 1, minWidth: 240, maxWidth: 360 }}>
          <Macro
            kcal={Math.round(totals.kcal / Math.max(1, totals.days))}
            p={Math.round(totals.p / Math.max(1, totals.days))}
            f={Math.round(totals.f / Math.max(1, totals.days))}
            c={Math.round(totals.c / Math.max(1, totals.days))}
            variant={tweaks.macroViz || 'progress'}
            target={macroTargets}
          />
        </div>
        <div className="week-strip-stats">
          <div>
            <span className="mono" style={{ fontSize: 18 }}>
              {totals.meals}
            </span>
            <br />
            <span style={{ fontSize: 11, color: 'var(--ink-faint)' }}>posiłków</span>
          </div>
          <div>
            <span className="mono" style={{ fontSize: 18 }}>
              {recipes.length}
            </span>
            <br />
            <span style={{ fontSize: 11, color: 'var(--ink-faint)' }}>przepisów</span>
          </div>
          <div>
            <span className="mono" style={{ fontSize: 18 }}>
              {totals.days}/7
            </span>
            <br />
            <span style={{ fontSize: 11, color: 'var(--ink-faint)' }}>dni planu</span>
          </div>
        </div>
      </div>

      {isNarrow && <WeekDotStrip />}
      {layout === 'rows' ? renderRows() : layout === 'compact' ? renderGrid(true) : renderGrid(false)}

      <div className="library">
        <div className="library-head">
          <h3 className="serif" style={{ fontStyle: 'italic' }}>
            Ulubione przepisy
          </h3>
          <span className="mono" style={{ fontSize: 11, color: 'var(--ink-faint)' }}>
            przeciągnij na dzień ↑
          </span>
        </div>
        <div className="library-strip">
          {libraryRecipes.length === 0 && (
            <div style={{ color: 'var(--ink-faint)', fontSize: 13, padding: '8px 0' }}>
              Brak ulubionych przepisów. Dodaj ulubione w sekcji Przepisy.
            </div>
          )}
          {libraryRecipes.map((r) => (
            <div
              key={r.id}
              className="lib-card"
              draggable={true}
              onDragStart={(ev) => {
                dragRef.current = { fromKey: 'library', recipe_id: r.id, servings: 1 };
                ev.dataTransfer.effectAllowed = 'copy';
                try {
                  ev.dataTransfer.setData('text/plain', r.id);
                } catch {
                  /* noop */
                }
              }}
              onDragEnd={() => {
                dragRef.current = null;
                setOver(null);
              }}
              onClick={() => openRecipe(r.id)}
            >
              <RecipeThumb recipe={r} h={64} />
              <div className="lib-card-title">{r.title}</div>
              <div className="mono" style={{ fontSize: 10, color: 'var(--ink-faint)' }}>
                {Math.round(r.kcal / r.servings)} kcal · {r.prep_time + r.cook_time}min
              </div>
            </div>
          ))}
        </div>
      </div>

      {picker && (
        <div
          className="modal-bg"
          onClick={() => {
            setPicker(null);
            setPickerQ('');
          }}
        >
          <div className="modal card" onClick={(e) => e.stopPropagation()}>
            <div className="modal-head">
              <div>
                <div className="eyebrow">Dodaj posiłek</div>
                <h2 className="serif" style={{ fontStyle: 'italic' }}>
                  {picker.meal} · {DAYS[picker.day]}
                </h2>
              </div>
              <button
                className="btn ghost icon"
                onClick={() => {
                  setPicker(null);
                  setPickerQ('');
                }}
              >
                <Icon name="x" size={16} />
              </button>
            </div>
            <div style={{ padding: '10px 14px', borderBottom: '1px solid var(--line-soft)' }}>
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 8,
                  background: 'var(--paper-2)',
                  border: '1px solid var(--line-soft)',
                  borderRadius: 'var(--r)',
                  padding: '8px 12px',
                }}
              >
                <Icon name="search" size={14} />
                <input
                  autoFocus
                  value={pickerQ}
                  onChange={(e) => setPickerQ(e.target.value)}
                  placeholder="Szukaj przepisu lub tagu…"
                  style={{
                    border: 0,
                    background: 'transparent',
                    outline: 'none',
                    flex: 1,
                    fontSize: 13,
                  }}
                />
                {pickerQ && (
                  <button
                    className="btn ghost icon"
                    style={{ padding: 2 }}
                    onClick={() => setPickerQ('')}
                  >
                    <Icon name="x" size={12} />
                  </button>
                )}
              </div>
            </div>
            <div className="modal-list">
              {pickerList.length === 0 ? (
                <div
                  style={{
                    padding: '32px 16px',
                    textAlign: 'center',
                    color: 'var(--ink-faint)',
                    fontSize: 13,
                  }}
                >
                  Brak przepisów pasujących do <span className="mono">"{pickerQ}"</span>
                </div>
              ) : (
                pickerList.map((r) => (
                  <button
                    key={r.id}
                    className="picker-row"
                    onClick={() => addRecipe(picker.day, picker.meal, r.id)}
                  >
                    <RecipeThumb recipe={r} h={48} />
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontWeight: 500 }}>{r.title}</div>
                      <div className="macro-row" style={{ marginTop: 3 }}>
                        <span className="mono">{Math.round(r.kcal / r.servings)} kcal</span>
                        <span style={{ opacity: 0.4 }}>·</span>
                        <span>
                          {r.tags.slice(0, 2).map((t) => (
                            <span
                              key={t}
                              className="chip"
                              style={{ marginRight: 4, fontSize: 10, padding: '1px 7px' }}
                            >
                              {t}
                            </span>
                          ))}
                        </span>
                      </div>
                    </div>
                    <Icon name="plus" size={14} />
                  </button>
                ))
              )}
            </div>
          </div>
        </div>
      )}

      {showTemplates && (
        <div className="modal-bg" onClick={() => setShowTemplates(false)}>
          <div className="modal card" style={{ maxWidth: 480 }} onClick={(e) => e.stopPropagation()}>
            <div className="modal-head">
              <div>
                <div className="eyebrow">Plany tygodniowe</div>
                <h2 className="serif" style={{ fontStyle: 'italic' }}>
                  Szablony tygodni
                </h2>
              </div>
              <button className="btn ghost icon" onClick={() => setShowTemplates(false)}>
                <Icon name="x" size={16} />
              </button>
            </div>

            <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--line-soft)' }}>
              <div style={{ fontSize: 12, color: 'var(--ink-3)', marginBottom: 8 }}>
                Zapisz bieżący tydzień ({fmtWeek(weekStart)}) jako szablon:
              </div>
              <div style={{ display: 'flex', gap: 8 }}>
                <input
                  value={saveTemplateName}
                  onChange={(e) => setSaveTemplateName(e.target.value)}
                  onKeyDown={(e) => { if (e.key === 'Enter') void handleSaveTemplate(); }}
                  placeholder="np. Tydzień azjatycki, przed zawodami…"
                  style={{
                    flex: 1,
                    border: '1px solid var(--line-soft)',
                    borderRadius: 'var(--r)',
                    padding: '7px 10px',
                    fontSize: 13,
                    background: 'var(--paper-2)',
                    outline: 'none',
                  }}
                />
                <button
                  className="btn primary"
                  disabled={!saveTemplateName.trim() || savingTemplate || plan.length === 0}
                  onClick={() => void handleSaveTemplate()}
                >
                  {savingTemplate ? '…' : 'Zapisz'}
                </button>
              </div>
              {plan.length === 0 && (
                <div style={{ fontSize: 11, color: 'var(--ink-faint)', marginTop: 6 }}>
                  Bieżący tydzień jest pusty — dodaj posiłki, aby zapisać szablon.
                </div>
              )}
            </div>

            <div className="modal-list" style={{ maxHeight: 360 }}>
              {templates.length === 0 ? (
                <div style={{ padding: '32px 16px', textAlign: 'center', color: 'var(--ink-faint)', fontSize: 13 }}>
                  Brak zapisanych szablonów
                </div>
              ) : (
                templates.map((tpl) => (
                  <div key={tpl.id} className="picker-row" style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontWeight: 500 }}>{tpl.name}</div>
                      <div style={{ fontSize: 11, color: 'var(--ink-faint)', marginTop: 2 }}>
                        {tpl.entries.length} posiłków ·{' '}
                        {new Set(tpl.entries.map((e) => e.day)).size}/7 dni ·{' '}
                        {new Date(tpl.created_at).toLocaleDateString('pl-PL')}
                      </div>
                    </div>
                    <button
                      className="btn primary"
                      style={{ flexShrink: 0 }}
                      disabled={applyingId === tpl.id}
                      onClick={() => void handleApplyTemplate(tpl.id)}
                    >
                      {applyingId === tpl.id ? '…' : 'Zastosuj'}
                    </button>
                    <button
                      className="btn ghost icon"
                      style={{ flexShrink: 0, color: 'var(--ink-faint)' }}
                      onClick={() => void handleDeleteTemplate(tpl.id)}
                      aria-label="Usuń szablon"
                    >
                      <Icon name="x" size={13} />
                    </button>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      )}

      {showGoals && (
        <div className="modal-bg" onClick={() => setShowGoals(false)}>
          <div className="modal card" style={{ maxWidth: 420 }} onClick={(e) => e.stopPropagation()}>
            <div className="modal-head">
              <div>
                <div className="eyebrow">Żywienie</div>
                <h2 className="serif" style={{ fontStyle: 'italic' }}>Cele dzienne</h2>
              </div>
              <button className="btn ghost icon" onClick={() => setShowGoals(false)}>
                <Icon name="x" size={16} />
              </button>
            </div>
            <div style={{ padding: '16px 20px', display: 'flex', flexDirection: 'column', gap: 14 }}>
              <div style={{ fontSize: 12, color: 'var(--ink-3)', lineHeight: 1.5 }}>
                Ustaw dzienny target. Asystent AI może sam dobierać przepisy, żeby trafić w te cele.
              </div>
              {(
                [
                  { key: 'kcal', label: 'Kalorie', unit: 'kcal', color: 'var(--macro-k)', min: 500, max: 6000, step: 50 },
                  { key: 'p', label: 'Białko', unit: 'g', color: 'var(--macro-p)', min: 0, max: 500, step: 5 },
                  { key: 'f', label: 'Tłuszcz', unit: 'g', color: 'var(--macro-f)', min: 0, max: 300, step: 5 },
                  { key: 'c', label: 'Węglowodany', unit: 'g', color: 'var(--macro-c)', min: 0, max: 800, step: 5 },
                ] as const
              ).map(({ key, label, unit, color, min, max, step }) => (
                <div key={key} style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <label className="field-label" style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                      <span
                        style={{
                          display: 'inline-block',
                          width: 8,
                          height: 8,
                          borderRadius: 99,
                          background: color,
                          flexShrink: 0,
                        }}
                      />
                      {label}
                    </label>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                      <input
                        type="number"
                        min={min}
                        max={max}
                        step={step}
                        value={goalsForm[key]}
                        onChange={(e) => setGoalsForm((prev) => ({ ...prev, [key]: Math.max(0, +e.target.value) }))}
                        style={{
                          width: 80,
                          border: '1px solid var(--line-soft)',
                          borderRadius: 'var(--r)',
                          padding: '5px 8px',
                          fontSize: 13,
                          background: 'var(--paper-2)',
                          outline: 'none',
                          textAlign: 'right',
                          fontFamily: 'var(--mono)',
                        }}
                      />
                      <span className="mono" style={{ fontSize: 11, color: 'var(--ink-faint)', width: 28 }}>{unit}</span>
                    </div>
                  </div>
                  <input
                    type="range"
                    min={min}
                    max={max}
                    step={step}
                    value={goalsForm[key]}
                    onChange={(e) => setGoalsForm((prev) => ({ ...prev, [key]: +e.target.value }))}
                    style={{ width: '100%', accentColor: color }}
                  />
                </div>
              ))}
              <div style={{ borderTop: '1px solid var(--line-soft)', paddingTop: 12, display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
                <button className="btn ghost" onClick={() => setShowGoals(false)}>Anuluj</button>
                <button className="btn primary" disabled={goalsSaving} onClick={() => void handleSaveGoals()}>
                  {goalsSaving ? 'Zapisywanie…' : 'Zapisz cele'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {showMealConfig && (
        <div className="modal-bg" onClick={() => setShowMealConfig(false)}>
          <div
            className="modal card"
            style={{ maxWidth: 440 }}
            onClick={(e) => e.stopPropagation()}
          >
            <div className="modal-head">
              <div>
                <div className="eyebrow">Konfiguracja</div>
                <h2 className="serif" style={{ fontStyle: 'italic' }}>
                  Typy posiłków
                </h2>
              </div>
              <button className="btn ghost icon" onClick={() => setShowMealConfig(false)}>
                <Icon name="x" size={16} />
              </button>
            </div>
            <div style={{ padding: '4px 16px 16px' }}>
              <div style={{ fontSize: 13, color: 'var(--ink-3)', padding: '10px 4px 14px' }}>
                Wybierz typy posiłków, które pojawią się jako wiersze w Kanbanie.
              </div>
              {MEAL_TYPES_ALL.map((m) => {
                const on = enabledMeals.includes(m.id);
                return (
                  <label key={m.id} className="meal-config-row">
                    <input type="checkbox" checked={on} onChange={() => toggleMeal(m.id)} />
                    <span
                      className="serif"
                      style={{ fontStyle: 'italic', fontSize: 16, flex: 1 }}
                    >
                      {m.id}
                    </span>
                    {plan.some((p) => p.meal === m.id) && (
                      <span
                        className="mono"
                        style={{ fontSize: 11, color: 'var(--ink-faint)' }}
                      >
                        {plan.filter((p) => p.meal === m.id).length} zaplanowanych
                      </span>
                    )}
                  </label>
                );
              })}
              <div
                style={{
                  fontSize: 12,
                  color: 'var(--ink-faint)',
                  padding: '12px 4px 4px',
                  borderTop: '1px dashed var(--line-soft)',
                  marginTop: 8,
                }}
              >
                Wyłączenie typu usuwa zaplanowane posiłki tego typu z bieżącego tygodnia.
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
