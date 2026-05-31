import { useCallback, useEffect, useRef, useState } from 'react';
import { Icon } from '../components/Icon';
import type { Recipe } from '../types';

interface CookingModeProps {
  recipe: Recipe;
  onClose: () => void;
}

interface StepTimer {
  sec: number;
  running: boolean;
  done: boolean;
}

type TimerMap = Record<number, StepTimer>;

function formatTime(sec: number): string {
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}:${s.toString().padStart(2, '0')}`;
}

function playTimerDone() {
  try {
    const ctx = new AudioContext();
    const notes = [880, 1046, 1318];
    notes.forEach((freq, i) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.type = 'sine';
      osc.frequency.value = freq;
      const t = ctx.currentTime + i * 0.18;
      gain.gain.setValueAtTime(0, t);
      gain.gain.linearRampToValueAtTime(0.4, t + 0.02);
      gain.gain.exponentialRampToValueAtTime(0.001, t + 0.4);
      osc.start(t);
      osc.stop(t + 0.4);
    });
  } catch {
    // AudioContext niedostępny
  }
}

export function CookingMode({ recipe, onClose }: CookingModeProps) {
  const steps = recipe.steps;
  const [stepIdx, setStepIdx] = useState(0);
  const [showIngredients, setShowIngredients] = useState(false);
  const [timers, setTimers] = useState<TimerMap>({});
  const intervalsRef = useRef<Record<number, ReturnType<typeof setInterval>>>({});
  const touchStartX = useRef<number | null>(null);

  const step = steps[stepIdx];
  const isFirst = stepIdx === 0;
  const isLast = stepIdx === steps.length - 1;

  // Cleanup all intervals on unmount
  useEffect(() => {
    return () => {
      Object.values(intervalsRef.current).forEach(clearInterval);
    };
  }, []);

  const initialSec = useCallback(
    (idx: number) => (steps[idx]?.duration_minutes ?? 0) * 60,
    [steps],
  );

  const getTimer = useCallback(
    (idx: number): StepTimer =>
      timers[idx] ?? { sec: initialSec(idx), running: false, done: false },
    [timers, initialSec],
  );

  const startTimer = useCallback(
    (idx: number) => {
      if (intervalsRef.current[idx]) return;
      setTimers((prev) => {
        const t = prev[idx] ?? { sec: initialSec(idx), running: false, done: false };
        const sec = t.sec <= 0 ? initialSec(idx) : t.sec;
        return { ...prev, [idx]: { sec, running: true, done: false } };
      });
      intervalsRef.current[idx] = setInterval(() => {
        setTimers((prev) => {
          const t = prev[idx];
          if (!t || t.sec <= 1) {
            clearInterval(intervalsRef.current[idx]);
            delete intervalsRef.current[idx];
            playTimerDone();
            return { ...prev, [idx]: { sec: 0, running: false, done: true } };
          }
          return { ...prev, [idx]: { ...t, sec: t.sec - 1 } };
        });
      }, 1000);
    },
    [initialSec],
  );

  const pauseTimer = useCallback((idx: number) => {
    clearInterval(intervalsRef.current[idx]);
    delete intervalsRef.current[idx];
    setTimers((prev) => ({
      ...prev,
      [idx]: { ...(prev[idx] ?? { sec: 0, running: false, done: false }), running: false },
    }));
  }, []);

  const resetTimer = useCallback(
    (idx: number) => {
      clearInterval(intervalsRef.current[idx]);
      delete intervalsRef.current[idx];
      setTimers((prev) => ({
        ...prev,
        [idx]: { sec: initialSec(idx), running: false, done: false },
      }));
    },
    [initialSec],
  );

  const goTo = useCallback((idx: number) => setStepIdx(idx), []);
  const goNext = useCallback(
    () => setStepIdx((i) => Math.min(i + 1, steps.length - 1)),
    [steps.length],
  );
  const goPrev = useCallback(() => setStepIdx((i) => Math.max(i - 1, 0)), []);

  // Keyboard navigation
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'ArrowRight' || e.key === 'ArrowDown') goNext();
      else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') goPrev();
      else if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [goNext, goPrev, onClose]);

  // Swipe
  const onTouchStart = (e: React.TouchEvent) => {
    touchStartX.current = e.touches[0].clientX;
  };
  const onTouchEnd = (e: React.TouchEvent) => {
    if (touchStartX.current === null) return;
    const dx = e.changedTouches[0].clientX - touchStartX.current;
    if (Math.abs(dx) > 52) {
      if (dx < 0) goNext();
      else goPrev();
    }
    touchStartX.current = null;
  };

  if (steps.length === 0) return null;

  const currentTimer = getTimer(stepIdx);
  const initSec = initialSec(stepIdx);
  const timerChanged = currentTimer.sec !== initSec || currentTimer.running || currentTimer.done;
  const anyDone = Object.values(timers).some((t) => t.done);

  // Background timers: running or done on steps other than current
  const bgTimers = Object.entries(timers)
    .map(([k, t]) => ({ idx: parseInt(k), ...t }))
    .filter(({ idx, running, done }) => idx !== stepIdx && (running || done));

  return (
    <div
      className={`cooking-mode${anyDone ? ' cm-alarm' : ''}`}
      onTouchStart={onTouchStart}
      onTouchEnd={onTouchEnd}
    >
      {/* Header */}
      <div className="cm-header">
        <button className="btn ghost icon" onClick={onClose} title="Wyjdź z trybu gotowania">
          <Icon name="x" size={18} />
        </button>
        <div className="cm-title serif">{recipe.title}</div>
        <button
          className={`btn ghost cm-ing-btn${showIngredients ? ' active' : ''}`}
          onClick={() => setShowIngredients((v) => !v)}
          title="Pokaż składniki"
        >
          <Icon name="book" size={14} />
          <span className="cm-ing-label">Składniki</span>
        </button>
      </div>

      {/* Background timer chips */}
      {bgTimers.length > 0 && (
        <div className="cm-bg-timers">
          {bgTimers.map(({ idx, sec, running, done }) => (
            <button
              key={idx}
              className={`cm-bg-chip${done ? ' done' : ''}`}
              onClick={() => goTo(idx)}
              title={`Wróć do kroku ${idx + 1}`}
            >
              <Icon name="clock" size={12} />
              <span className="mono">{formatTime(sec)}</span>
              <span className="cm-bg-chip-label">krok {idx + 1}</span>
              {running && <span className="cm-bg-chip-dot" />}
            </button>
          ))}
        </div>
      )}

      {/* Progress dots */}
      <div className="cm-dots">
        {steps.map((s, i) => {
          const t = timers[i];
          const hasBg = i !== stepIdx && t && (t.running || t.done);
          return (
            <button
              key={i}
              className={`cm-dot${i === stepIdx ? ' active' : ''}${hasBg ? (t.done ? ' dot-done' : ' dot-running') : ''}`}
              onClick={() => goTo(i)}
              title={`Krok ${i + 1}${s.duration_minutes ? ` — ${s.duration_minutes} min` : ''}`}
            />
          );
        })}
      </div>

      {/* Step body */}
      <div className="cm-step">
        <div className="cm-step-n serif">{stepIdx + 1}</div>
        <p className="cm-step-text">{step.text}</p>

        {step.duration_minutes != null && (
          <div className="cm-timer">
            <div className={`cm-timer-display${currentTimer.done ? ' done' : ''}`}>
              {formatTime(currentTimer.sec)}
            </div>
            <div className="cm-timer-controls">
              {currentTimer.running ? (
                <button className="btn" onClick={() => pauseTimer(stepIdx)}>
                  Pauza
                </button>
              ) : (
                <button className="btn primary" onClick={() => startTimer(stepIdx)}>
                  <Icon name="clock" size={14} />
                  {currentTimer.done ? 'Ponów' : currentTimer.sec < initSec ? 'Wznów' : 'Start'}
                </button>
              )}
              {timerChanged && !currentTimer.running && (
                <button className="btn ghost" onClick={() => resetTimer(stepIdx)}>
                  Reset
                </button>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Navigation */}
      <div className="cm-nav">
        <button className="btn" onClick={goPrev} disabled={isFirst}>
          <Icon name="chev-l" size={16} />
          <span className="cm-nav-label">Poprzedni</span>
        </button>
        <span className="cm-nav-counter mono">
          {stepIdx + 1} / {steps.length}
        </span>
        {isLast ? (
          <button className="btn primary" onClick={onClose}>
            <Icon name="check" size={16} />
            <span className="cm-nav-label">Gotowe!</span>
          </button>
        ) : (
          <button className="btn primary" onClick={goNext}>
            <span className="cm-nav-label">Następny</span>
            <Icon name="chev-r" size={16} />
          </button>
        )}
      </div>

      {/* Ingredients panel */}
      {showIngredients && (
        <div className="cm-ingredients-panel" onClick={() => setShowIngredients(false)}>
          <div className="cm-ingredients-sheet" onClick={(e) => e.stopPropagation()}>
            <div className="cm-ingredients-head">
              <span className="eyebrow">Składniki — {recipe.servings} porcji</span>
              <button className="btn ghost icon" onClick={() => setShowIngredients(false)}>
                <Icon name="x" size={14} />
              </button>
            </div>
            <ul className="rd-ingredients">
              {recipe.ingredients.map((ing, i) => (
                <li key={i}>
                  <span
                    className="mono"
                    style={{ color: 'var(--ink-2)', minWidth: 72, display: 'inline-block' }}
                  >
                    {ing.qty} {ing.unit}
                  </span>
                  <span>{ing.name}</span>
                </li>
              ))}
            </ul>
          </div>
        </div>
      )}
    </div>
  );
}
