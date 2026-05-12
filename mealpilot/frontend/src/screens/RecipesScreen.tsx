import { useEffect, useMemo, useRef, useState, type ChangeEvent, type ReactNode } from 'react';
import { Icon } from '../components/Icon';
import { Macro } from '../components/Macro';
import { RecipeThumb } from '../components/RecipeThumb';
import {
  MEAL_TYPES_ALL,
  RECIPES_CHANGED,
  createRecipe,
  deleteRecipe,
  deleteRecipeImage,
  emitRecipesChanged,
  estimateMacros,
  getRecipes,
  loadRecipes,
  recipeBy,
  recipeImageUrl,
  updateRecipe,
  updateRecipeOwnership,
  uploadRecipeImage,
} from '../data';
import type { Ingredient, Recipe } from '../types';

const INGREDIENT_UNITS = [
  'g',
  'kg',
  'ml',
  'l',
  'szt',
  'łyżka',
  'łyżeczka',
  'szklanka',
  'szczypta',
  'ząbek',
  'kromka',
] as const;

interface TagInputProps {
  value: string[];
  onChange: (next: string[]) => void;
  suggestions: string[];
}

type TagItem = string | { create: true; label: string };

function TagInput({ value, onChange, suggestions }: TagInputProps) {
  const [input, setInput] = useState('');
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const wrapRef = useRef<HTMLDivElement | null>(null);

  const q = input.trim().toLowerCase();
  const filtered = suggestions
    .filter((t) => !value.includes(t))
    .filter((t) => (q === '' ? true : t.toLowerCase().includes(q)))
    .slice(0, 8);
  const exactMatch = filtered.some((t) => t.toLowerCase() === q);
  const showCreate = q.length > 0 && !exactMatch && !value.includes(input.trim());
  const items: TagItem[] = [
    ...filtered,
    ...(showCreate ? [{ create: true as const, label: input.trim() }] : []),
  ];

  const addTag = (t: string) => {
    const clean = (t || '').trim();
    if (!clean || value.includes(clean)) {
      setInput('');
      return;
    }
    onChange([...value, clean]);
    setInput('');
    setActive(0);
  };
  const removeTag = (t: string) => onChange(value.filter((x) => x !== t));

  const onKey = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' || e.key === ',') {
      e.preventDefault();
      const pick = items[active];
      if (pick) addTag(typeof pick === 'string' ? pick : pick.label);
      else if (input.trim()) addTag(input);
    } else if (e.key === 'Backspace' && input === '' && value.length) {
      removeTag(value[value.length - 1]);
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActive((a) => Math.min(a + 1, Math.max(items.length - 1, 0)));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActive((a) => Math.max(a - 1, 0));
    } else if (e.key === 'Escape') {
      setOpen(false);
    }
  };

  useEffect(() => {
    const onDoc = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, []);
  useEffect(() => {
    setActive(0);
  }, [input]);

  return (
    <div ref={wrapRef} style={{ position: 'relative' }}>
      <div
        className="edit-input"
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: 4,
          alignItems: 'center',
          cursor: 'text',
          padding: '4px 6px',
          minHeight: 32,
        }}
        onClick={() => wrapRef.current?.querySelector('input')?.focus()}
      >
        {value.map((t) => (
          <span
            key={t}
            className="chip"
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 4,
              fontSize: 12,
              padding: '2px 8px',
            }}
          >
            {t}
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                removeTag(t);
              }}
              style={{
                border: 0,
                background: 'transparent',
                cursor: 'pointer',
                padding: 0,
                display: 'inline-flex',
                color: 'inherit',
              }}
            >
              <Icon name="x" size={10} />
            </button>
          </span>
        ))}
        <input
          value={input}
          onChange={(e) => {
            setInput(e.target.value);
            setOpen(true);
          }}
          onFocus={() => setOpen(true)}
          onKeyDown={onKey}
          placeholder={value.length ? '' : 'np. obiad, ryby, szybkie'}
          style={{
            border: 0,
            outline: 'none',
            background: 'transparent',
            flex: '1 1 100px',
            minWidth: 80,
            fontSize: 13,
            padding: '2px 4px',
          }}
        />
      </div>
      {open && items.length > 0 && (
        <div
          style={{
            position: 'absolute',
            top: '100%',
            left: 0,
            right: 0,
            marginTop: 4,
            background: 'var(--card)',
            border: '1px solid var(--line)',
            borderRadius: 'var(--r)',
            boxShadow: 'var(--shadow)',
            zIndex: 10,
            maxHeight: 220,
            overflowY: 'auto',
          }}
        >
          {items.map((it, i) => {
            const isCreate = typeof it !== 'string';
            const label = isCreate ? it.label : it;
            return (
              <div
                key={(isCreate ? 'new:' : '') + label}
                onMouseDown={(e) => {
                  e.preventDefault();
                  addTag(label);
                }}
                onMouseEnter={() => setActive(i)}
                style={{
                  padding: '6px 10px',
                  cursor: 'pointer',
                  fontSize: 13,
                  display: 'flex',
                  alignItems: 'center',
                  gap: 8,
                  background: i === active ? 'var(--paper-2)' : 'transparent',
                }}
              >
                {isCreate ? (
                  <>
                    <Icon name="plus" size={11} />
                    <span style={{ color: 'var(--ink-3)' }}>Utwórz tag</span>
                    <span className="chip" style={{ fontSize: 11, padding: '1px 7px' }}>
                      {label}
                    </span>
                  </>
                ) : (
                  <span className="chip" style={{ fontSize: 11, padding: '1px 7px' }}>
                    {label}
                  </span>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

interface MealTypePickerProps {
  value: string[];
  onChange: (next: string[]) => void;
  suggestions: string[];
}

function MealTypePicker({ value, onChange, suggestions }: MealTypePickerProps) {
  return (
    <TagInput
      value={value}
      onChange={onChange}
      suggestions={suggestions}
    />
  );
}

function LabelField({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
      <span className="eyebrow">{label}</span>
      {children}
    </label>
  );
}

type RecipeForm = Omit<Recipe, 'id'>;

const emptyForm: RecipeForm = {
  title: '',
  tags: [],
  servings: 2,
  prep_time: 0,
  cook_time: 0,
  kcal: 0,
  p: 0,
  f: 0,
  c: 0,
  hue: 40,
  ingredients: [],
  steps: [],
  meal_types: [],
};

interface NewRecipeModalProps {
  onClose: () => void;
  onSave: (payload: Recipe) => Promise<void>;
}

function NewRecipeModal({ onClose, onSave }: NewRecipeModalProps) {
  const existingTags = useMemo(
    () => [...new Set(getRecipes().flatMap((r) => r.tags || []))].sort(),
    [],
  );
  const mealTypeSuggestions = useMemo(
    () =>
      [
        ...new Set([
          ...MEAL_TYPES_ALL.map((m) => m.id),
          ...getRecipes().flatMap((r) => r.meal_types || []),
        ]),
      ].sort(),
    [],
  );
  const [form, setForm] = useState<RecipeForm>(emptyForm);
  const [saving, setSaving] = useState(false);
  const [estimating, setEstimating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const set = <K extends keyof RecipeForm>(k: K, v: RecipeForm[K]) =>
    setForm((f) => ({ ...f, [k]: v }));
  const num = (k: keyof RecipeForm, v: string) => {
    const n = parseFloat(v) || 0;
    setForm((f) => ({ ...f, [k]: n }));
  };

  const addIng = () =>
    setForm((f) => ({ ...f, ingredients: [...f.ingredients, { name: '', qty: 0, unit: 'g' }] }));
  const updIng = (i: number, p: Partial<Ingredient>) =>
    setForm((f) => ({
      ...f,
      ingredients: f.ingredients.map((ing, idx) => (idx === i ? { ...ing, ...p } : ing)),
    }));
  const remIng = (i: number) =>
    setForm((f) => ({ ...f, ingredients: f.ingredients.filter((_, idx) => idx !== i) }));
  const addStep = () => setForm((f) => ({ ...f, steps: [...f.steps, ''] }));
  const updStep = (i: number, v: string) =>
    setForm((f) => ({ ...f, steps: f.steps.map((s, idx) => (idx === i ? v : s)) }));
  const remStep = (i: number) =>
    setForm((f) => ({ ...f, steps: f.steps.filter((_, idx) => idx !== i) }));

  const submit = async () => {
    if (!form.title.trim()) {
      setError('Wpisz nazwę przepisu.');
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await onSave({
        ...form,
        id: crypto.randomUUID(),
        servings: Math.trunc(form.servings) || 1,
        prep_time: Math.trunc(form.prep_time) || 0,
        cook_time: Math.trunc(form.cook_time) || 0,
        hue: Math.trunc(form.hue) || 40,
      });
    } catch (e) {
      setError('Nie udało się zapisać: ' + (e as Error).message);
      setSaving(false);
    }
  };

  return (
    <div className="modal-bg" onClick={onClose}>
      <div
        className="modal card"
        style={{ maxWidth: 680, maxHeight: '90vh', display: 'flex', flexDirection: 'column' }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="modal-head">
          <div>
            <div className="eyebrow">Przepisy</div>
            <h2 className="serif" style={{ fontStyle: 'italic' }}>
              Nowy przepis
            </h2>
          </div>
          <button className="btn ghost icon" onClick={onClose}>
            <Icon name="x" size={16} />
          </button>
        </div>
        <div
          style={{
            overflowY: 'auto',
            padding: '6px 18px 18px',
            display: 'flex',
            flexDirection: 'column',
            gap: 16,
          }}
        >
          <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            <div className="eyebrow" style={{ color: 'var(--ink-faint)' }}>
              Podstawowe
            </div>
            <LabelField label="Nazwa przepisu">
              <input
                autoFocus
                className="edit-input"
                placeholder="np. Makaron z łososiem"
                value={form.title}
                onChange={(e) => set('title', e.target.value)}
              />
            </LabelField>
            <LabelField label="Tagi">
              <TagInput
                value={form.tags}
                onChange={(v) => set('tags', v)}
                suggestions={existingTags}
              />
            </LabelField>
            <LabelField label="Typ posiłku">
              <MealTypePicker
                value={form.meal_types}
                onChange={(v) => set('meal_types', v)}
                suggestions={mealTypeSuggestions}
              />
            </LabelField>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 10 }}>
              <LabelField label="Porcje">
                <input
                  className="edit-input"
                  type="number"
                  min="1"
                  value={form.servings}
                  onChange={(e) => num('servings', e.target.value)}
                />
              </LabelField>
              <LabelField label="Czas przyg. (min)">
                <input
                  className="edit-input"
                  type="number"
                  min="0"
                  value={form.prep_time}
                  onChange={(e) => num('prep_time', e.target.value)}
                />
              </LabelField>
              <LabelField label="Czas got. (min)">
                <input
                  className="edit-input"
                  type="number"
                  min="0"
                  value={form.cook_time}
                  onChange={(e) => num('cook_time', e.target.value)}
                />
              </LabelField>
            </div>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            <div className="eyebrow" style={{ color: 'var(--ink-faint)' }}>
              Makro (łącznie dla wszystkich porcji)
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5,1fr)', gap: 10 }}>
              <LabelField label="Kcal">
                <input
                  className="edit-input"
                  type="number"
                  min="0"
                  value={form.kcal}
                  onChange={(e) => num('kcal', e.target.value)}
                />
              </LabelField>
              <LabelField label="Białko (g)">
                <input
                  className="edit-input"
                  type="number"
                  min="0"
                  value={form.p}
                  onChange={(e) => num('p', e.target.value)}
                />
              </LabelField>
              <LabelField label="Tłuszcz (g)">
                <input
                  className="edit-input"
                  type="number"
                  min="0"
                  value={form.f}
                  onChange={(e) => num('f', e.target.value)}
                />
              </LabelField>
              <LabelField label="Węglo (g)">
                <input
                  className="edit-input"
                  type="number"
                  min="0"
                  value={form.c}
                  onChange={(e) => num('c', e.target.value)}
                />
              </LabelField>
              <LabelField label=" ">
                <button
                  className="btn"
                  style={{ width: '100%', height: '100%' }}
                  disabled={estimating || form.ingredients.length === 0}
                  title={form.ingredients.length === 0 ? 'Dodaj składniki przed szacowaniem' : 'Oszacuj makra przez AI'}
                  onClick={async () => {
                    setEstimating(true);
                    setError(null);
                    try {
                      const est = await estimateMacros({
                        title: form.title || 'Przepis',
                        servings: form.servings,
                        ingredients: form.ingredients,
                      });
                      setForm((f) => ({ ...f, kcal: Math.round(est.kcal), p: Math.round(est.p), f: Math.round(est.f), c: Math.round(est.c) }));
                    } catch (e) {
                      setError('Szacowanie makr: ' + (e as Error).message);
                    } finally {
                      setEstimating(false);
                    }
                  }}
                >
                  {estimating ? '…' : 'Szacuj AI'}
                </button>
              </LabelField>
            </div>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            <div className="eyebrow" style={{ color: 'var(--ink-faint)' }}>
              Kolor karty
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <div
                style={{
                  width: 40,
                  height: 40,
                  borderRadius: 'var(--r)',
                  background: `oklch(0.93 0.06 ${form.hue})`,
                  border: '1px solid var(--line)',
                  flexShrink: 0,
                }}
              />
              <input
                type="range"
                min="0"
                max="360"
                value={form.hue}
                onChange={(e) => set('hue', parseInt(e.target.value))}
                style={{ flex: 1 }}
              />
              <span className="mono" style={{ fontSize: 12, color: 'var(--ink-3)', minWidth: 30 }}>
                {form.hue}°
              </span>
            </div>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            <div
              style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}
            >
              <div className="eyebrow" style={{ color: 'var(--ink-faint)' }}>
                Składniki
              </div>
              <button
                className="btn ghost"
                style={{ fontSize: 11, padding: '3px 8px' }}
                onClick={addIng}
              >
                <Icon name="plus" size={11} /> dodaj
              </button>
            </div>
            {form.ingredients.length === 0 && (
              <div style={{ fontSize: 12, color: 'var(--ink-faint)', fontStyle: 'italic' }}>
                Brak składników — kliknij „dodaj"
              </div>
            )}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {form.ingredients.map((ing, i) => (
                <div key={i} style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
                  <input
                    className="edit-num"
                    style={{ width: 64 }}
                    type="number"
                    step="0.1"
                    placeholder="ile"
                    value={ing.qty}
                    onChange={(e) => updIng(i, { qty: parseFloat(e.target.value) || 0 })}
                  />
                  <select
                    className="edit-input"
                    style={{ width: 90 }}
                    value={ing.unit}
                    onChange={(e) => updIng(i, { unit: e.target.value })}
                  >
                    {INGREDIENT_UNITS.map((u) => (
                      <option key={u} value={u}>
                        {u}
                      </option>
                    ))}
                  </select>
                  <input
                    className="edit-input"
                    style={{ flex: 1 }}
                    placeholder="nazwa składnika"
                    value={ing.name}
                    onChange={(e) => updIng(i, { name: e.target.value })}
                  />
                  <button
                    className="btn ghost icon"
                    style={{ padding: 2 }}
                    onClick={() => remIng(i)}
                  >
                    <Icon name="x" size={12} />
                  </button>
                </div>
              ))}
            </div>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            <div
              style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}
            >
              <div className="eyebrow" style={{ color: 'var(--ink-faint)' }}>
                Kroki przygotowania
              </div>
              <button
                className="btn ghost"
                style={{ fontSize: 11, padding: '3px 8px' }}
                onClick={addStep}
              >
                <Icon name="plus" size={11} /> dodaj
              </button>
            </div>
            {form.steps.length === 0 && (
              <div style={{ fontSize: 12, color: 'var(--ink-faint)', fontStyle: 'italic' }}>
                Brak kroków — kliknij „dodaj"
              </div>
            )}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {form.steps.map((s, i) => (
                <div key={i} style={{ display: 'flex', gap: 6, alignItems: 'flex-start' }}>
                  <span
                    className="mono"
                    style={{
                      fontSize: 12,
                      color: 'var(--ink-faint)',
                      minWidth: 20,
                      paddingTop: 8,
                    }}
                  >
                    {i + 1}.
                  </span>
                  <textarea
                    className="edit-input"
                    style={{ flex: 1, minHeight: 52, resize: 'vertical' }}
                    placeholder="Opisz krok…"
                    value={s}
                    onChange={(e) => updStep(i, e.target.value)}
                  />
                  <button
                    className="btn ghost icon"
                    style={{ padding: 2, marginTop: 6 }}
                    onClick={() => remStep(i)}
                  >
                    <Icon name="x" size={12} />
                  </button>
                </div>
              ))}
            </div>
          </div>

          {error && (
            <div
              style={{
                color: 'var(--terra)',
                fontSize: 13,
                padding: '8px 10px',
                background: 'oklch(0.97 0.02 15)',
                borderRadius: 'var(--r)',
                border: '1px solid oklch(0.88 0.04 15)',
              }}
            >
              {error}
            </div>
          )}

          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, paddingTop: 4 }}>
            <button className="btn ghost" onClick={onClose}>
              Anuluj
            </button>
            <button
              className="btn primary"
              onClick={submit}
              disabled={saving || !form.title.trim()}
            >
              {saving ? (
                'Zapisywanie…'
              ) : (
                <>
                  <Icon name="check" size={13} /> Zapisz przepis
                </>
              )}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

interface RecipeCardProps {
  recipe: Recipe;
  openRecipe: (id: string) => void;
  isFavorite: boolean;
  onToggleFavorite: (id: string) => void;
  currentUserId: number;
}

function RecipeCard({ recipe: r, openRecipe, isFavorite, onToggleFavorite, currentUserId }: RecipeCardProps) {
  const isHousehold = r.owner_household_id != null;
  const isCreator = r.created_by === currentUserId;
  const [busy, setBusy] = useState(false);
  const toggleOwnership = async () => {
    setBusy(true);
    try {
      await updateRecipeOwnership(r.id, !isHousehold);
      emitRecipesChanged();
    } catch (e) {
      alert(`Nie udało się zmienić widoczności: ${(e as Error).message}`);
    } finally {
      setBusy(false);
    }
  };
  return (
    <button
      key={r.id}
      className="recipe-card card paper-grain"
      onClick={() => openRecipe(r.id)}
      style={{ position: 'relative' }}
    >
      <RecipeThumb recipe={r} h={150} />
      <button
        className="btn icon"
        style={{
          position: 'absolute',
          top: 8,
          right: 8,
          background: 'oklch(1 0 0 / 0.75)',
          borderRadius: '50%',
          width: 30,
          height: 30,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: isFavorite ? 'oklch(0.55 0.22 15)' : 'var(--ink-3)',
        }}
        onClick={(e) => {
          e.stopPropagation();
          onToggleFavorite(r.id);
        }}
        title={isFavorite ? 'Usuń z ulubionych' : 'Dodaj do ulubionych'}
      >
        <Icon
          name="heart"
          size={14}
          filled={isFavorite}
        />
      </button>
      <span
        onClick={(e) => {
          e.stopPropagation();
          if (isCreator && !busy) void toggleOwnership();
        }}
        title={
          isCreator
            ? isHousehold
              ? 'Udostępniony grupie domowej — kliknij, aby zrobić prywatnym'
              : 'Prywatny — kliknij, aby udostępnić grupie domowej'
            : isHousehold
              ? 'Udostępniony grupie domowej'
              : 'Prywatny'
        }
        style={{
          position: 'absolute',
          top: 8,
          left: 8,
          fontSize: 10,
          padding: '2px 8px',
          borderRadius: 999,
          background: isHousehold ? 'var(--accent)' : 'oklch(1 0 0 / 0.75)',
          color: isHousehold ? 'oklch(0.98 0.015 80)' : 'var(--ink-2)',
          border: isHousehold ? '1px solid var(--accent-deep)' : '1px solid var(--line)',
          display: 'inline-flex',
          alignItems: 'center',
          gap: 4,
          cursor: isCreator ? 'pointer' : 'default',
          opacity: busy ? 0.6 : 1,
        }}
      >
        {isHousehold && <Icon name="users" size={10} />}
        {isHousehold ? 'Grupa domowa' : 'Prywatny'}
      </span>
      <div className="recipe-card-body">
        <div style={{ display: 'flex', gap: 6, marginBottom: 6, flexWrap: 'wrap' }}>
          {r.tags.slice(0, 3).map((t) => (
            <span key={t} className="chip" style={{ fontSize: 10, padding: '1px 7px' }}>
              {t}
            </span>
          ))}
        </div>
        <h3 className="serif">{r.title}</h3>
        <div
          style={{ display: 'flex', gap: 12, marginTop: 8, fontSize: 12, color: 'var(--ink-3)' }}
        >
          <span style={{ display: 'inline-flex', flexDirection: 'column', gap: 2 }}>
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
              <Icon name="clock" size={12} />
              przygotowanie <span className="mono">{r.prep_time}</span> min
            </span>
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, paddingLeft: 16 }}>
              gotowanie <span className="mono">{r.cook_time}</span> min
            </span>
          </span>
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
            <Icon name="users" size={12} />{' '}
            <span className="mono">{r.servings}</span> porcji
          </span>
        </div>
        <div style={{ marginTop: 10, paddingTop: 10, borderTop: '1px dashed var(--line-soft)' }}>
          <Macro
            kcal={Math.round(r.kcal / r.servings)}
            p={Math.round(r.p / r.servings)}
            f={Math.round(r.f / r.servings)}
            c={Math.round(r.c / r.servings)}
            variant="bar"
          />
        </div>
      </div>
    </button>
  );
}

interface RecipesScreenProps {
  openRecipe: (id: string) => void;
  grouped: boolean;
  onGroupedChange: (v: boolean) => void;
  favoriteIds: string[];
  onToggleFavorite: (id: string) => void;
  currentUserId: number;
}

export function RecipesScreen({ openRecipe, grouped, onGroupedChange, favoriteIds, onToggleFavorite, currentUserId }: RecipesScreenProps) {
  const [recipes, setRecipes] = useState<Recipe[]>(getRecipes());
  const [showNew, setShowNew] = useState(false);
  const [q, setQ] = useState('');
  const [tag, setTag] = useState('wszystkie');
  const [onlyFavorites, setOnlyFavorites] = useState(false);

  const refresh = () => loadRecipes().then(() => setRecipes([...getRecipes()]));

  useEffect(() => {
    const onChanged = () => setRecipes([...getRecipes()]);
    window.addEventListener(RECIPES_CHANGED, onChanged);
    return () => window.removeEventListener(RECIPES_CHANGED, onChanged);
  }, []);

  const allTags = ['wszystkie', ...new Set(recipes.flatMap((r) => r.tags))];
  const list = recipes.filter(
    (r) =>
      (tag === 'wszystkie' || r.tags.includes(tag)) &&
      (!onlyFavorites || favoriteIds.includes(r.id)) &&
      (q === '' || r.title.toLowerCase().includes(q.toLowerCase())),
  );

  const handleSave = async (payload: Recipe) => {
    await createRecipe(payload);
    await refresh();
    setShowNew(false);
  };

  const mealTypeGroups = useMemo(() => {
    const knownTypes = MEAL_TYPES_ALL.map((m) => m.id);
    const assignedIds = new Set<string>();
    const groups: { label: string; items: Recipe[] }[] = knownTypes.map((mt) => {
      const items = list.filter((r) => (r.meal_types || []).includes(mt));
      items.forEach((r) => assignedIds.add(r.id));
      return { label: mt, items };
    });
    const unassigned = list.filter((r) => !assignedIds.has(r.id));
    if (unassigned.length > 0) groups.push({ label: 'Bez przypisania', items: unassigned });
    return groups.filter((g) => g.items.length > 0);
  }, [list]);

  return (
    <div>
      <div className="page-head">
        <div>
          <div className="eyebrow">{recipes.length} pozycji w bibliotece</div>
          <h1 className="serif" style={{ fontStyle: 'italic' }}>
            Przepisy
          </h1>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button
            className={`btn${onlyFavorites ? ' primary' : ' ghost'}`}
            onClick={() => setOnlyFavorites((v) => !v)}
            title="Pokaż tylko ulubione"
          >
            <Icon name="heart" size={14} /> Ulubione
            {favoriteIds.length > 0 && (
              <span className="badge" style={{ marginLeft: 4 }}>{favoriteIds.length}</span>
            )}
          </button>
          <button
            className={`btn${grouped ? ' primary' : ' ghost'}`}
            onClick={() => onGroupedChange(!grouped)}
            title="Grupuj według typu posiłku"
          >
            <Icon name="layout-list" size={14} /> Grupuj
          </button>
          <button className="btn primary" onClick={() => setShowNew(true)}>
            <Icon name="plus" size={14} /> Nowy przepis
          </button>
        </div>
      </div>
      {showNew && <NewRecipeModal onClose={() => setShowNew(false)} onSave={handleSave} />}

      <div style={{ display: 'flex', gap: 12, marginBottom: 18, flexWrap: 'wrap' }}>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            background: 'var(--card)',
            border: '1px solid var(--line)',
            borderRadius: 'var(--r)',
            padding: '6px 10px',
            minWidth: 240,
            flex: '1 1 240px',
            maxWidth: 380,
          }}
        >
          <Icon name="search" size={14} />
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Szukaj po nazwie…"
            style={{
              border: 0,
              background: 'transparent',
              outline: 'none',
              flex: 1,
              fontSize: 13,
            }}
          />
        </div>
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          {allTags.map((t) => (
            <button
              key={t}
              className="chip"
              onClick={() => setTag(t)}
              style={{
                cursor: 'pointer',
                background: tag === t ? 'var(--accent)' : undefined,
                color: tag === t ? 'oklch(0.98 0.015 80)' : undefined,
                borderColor: tag === t ? 'var(--accent-deep)' : undefined,
              }}
            >
              {t}
            </button>
          ))}
        </div>
      </div>

      {grouped ? (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 32 }}>
          {mealTypeGroups.map((group) => (
            <div key={group.label}>
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 10,
                  marginBottom: 14,
                }}
              >
                <h2
                  className="serif"
                  style={{ fontStyle: 'italic', fontSize: 22, margin: 0 }}
                >
                  {group.label}
                </h2>
                <span
                  className="mono"
                  style={{ fontSize: 11, color: 'var(--ink-faint)', paddingTop: 2 }}
                >
                  {group.items.length}
                </span>
                <div style={{ flex: 1, height: 1, background: 'var(--line-soft)' }} />
              </div>
              <div className="recipe-grid">
                {group.items.map((r) => (
                  <RecipeCard key={r.id} recipe={r} openRecipe={openRecipe} isFavorite={favoriteIds.includes(r.id)} onToggleFavorite={onToggleFavorite} currentUserId={currentUserId} />
                ))}
              </div>
            </div>
          ))}
          {mealTypeGroups.length === 0 && (
            <div style={{ color: 'var(--ink-faint)', fontStyle: 'italic', fontSize: 14 }}>
              Brak przepisów spełniających kryteria.
            </div>
          )}
        </div>
      ) : (
        <div className="recipe-grid">
          {list.map((r) => (
            <RecipeCard key={r.id} recipe={r} openRecipe={openRecipe} isFavorite={favoriteIds.includes(r.id)} onToggleFavorite={onToggleFavorite} currentUserId={currentUserId} />
          ))}
        </div>
      )}
    </div>
  );
}

interface RecipeDetailProps {
  recipeId: string;
  onClose: () => void;
  isFavorite: boolean;
  onToggleFavorite: (id: string) => void;
  currentUserId: number;
}

export function RecipeDetail({ recipeId, onClose, isFavorite, onToggleFavorite, currentUserId }: RecipeDetailProps) {
  const [ownershipBusy, setOwnershipBusy] = useState(false);
  const baseR = recipeBy(recipeId);
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState<Recipe | undefined>(baseR);
  const [saving, setSaving] = useState(false);
  const [estimating, setEstimating] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [imageBusy, setImageBusy] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const onPickImage = () => fileInputRef.current?.click();

  const onImageChosen = async (e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    e.target.value = '';
    if (!file || !baseR) return;
    setImageBusy(true);
    try {
      const updated = await uploadRecipeImage(baseR.id, file);
      Object.assign(baseR, updated);
      setDraft((d) => (d ? { ...d, image_filename: updated.image_filename } : d));
      emitRecipesChanged();
    } catch (err) {
      alert(`Nie udało się wgrać zdjęcia: ${(err as Error).message}`);
    } finally {
      setImageBusy(false);
    }
  };

  const onRemoveImage = async () => {
    if (!baseR?.image_filename) return;
    if (!confirm('Usunąć zdjęcie tego przepisu?')) return;
    setImageBusy(true);
    try {
      const updated = await deleteRecipeImage(baseR.id);
      Object.assign(baseR, updated);
      setDraft((d) => (d ? { ...d, image_filename: updated.image_filename } : d));
      emitRecipesChanged();
    } catch (err) {
      alert(`Nie udało się usunąć zdjęcia: ${(err as Error).message}`);
    } finally {
      setImageBusy(false);
    }
  };

  useEffect(() => {
    setDraft(baseR);
    setEditing(false);
  }, [recipeId, baseR]);

  const existingTags = useMemo(
    () => [...new Set(getRecipes().flatMap((r) => r.tags || []))].sort(),
    [],
  );
  const mealTypeSuggestions = useMemo(
    () =>
      [
        ...new Set([
          ...MEAL_TYPES_ALL.map((m) => m.id),
          ...getRecipes().flatMap((r) => r.meal_types || []),
        ]),
      ].sort(),
    [],
  );

  if (!baseR || !draft) return null;
  const r = editing ? draft : baseR;

  const updateIng = (i: number, patch: Partial<Ingredient>) =>
    setDraft((d) =>
      d
        ? {
            ...d,
            ingredients: d.ingredients.map((ing, idx) => (idx === i ? { ...ing, ...patch } : ing)),
          }
        : d,
    );
  const removeIng = (i: number) =>
    setDraft((d) =>
      d ? { ...d, ingredients: d.ingredients.filter((_, idx) => idx !== i) } : d,
    );
  const addIng = () =>
    setDraft((d) =>
      d ? { ...d, ingredients: [...d.ingredients, { name: '', qty: 0, unit: 'g' }] } : d,
    );
  const updateStep = (i: number, val: string) =>
    setDraft((d) =>
      d ? { ...d, steps: d.steps.map((s, idx) => (idx === i ? val : s)) } : d,
    );
  const removeStep = (i: number) =>
    setDraft((d) => (d ? { ...d, steps: d.steps.filter((_, idx) => idx !== i) } : d));
  const addStep = () => setDraft((d) => (d ? { ...d, steps: [...d.steps, ''] } : d));

  const save = async () => {
    setSaving(true);
    setSaveError(null);
    try {
      const { id: _id, ...payload } = draft;
      void _id;
      await updateRecipe(baseR.id, payload);
      Object.assign(baseR, draft);
      emitRecipesChanged();
      setEditing(false);
    } catch (e) {
      setSaveError((e as Error).message);
    } finally {
      setSaving(false);
    }
  };
  const cancel = () => {
    setDraft(baseR);
    setEditing(false);
    setSaveError(null);
  };
  const remove = async () => {
    if (
      !confirm(
        `Usunąć przepis „${baseR.title}"? Zostanie też usunięty ze wszystkich planów tygodnia.`,
      )
    )
      return;
    setDeleting(true);
    try {
      await deleteRecipe(baseR.id);
      emitRecipesChanged();
      onClose();
    } catch (e) {
      alert(`Nie udało się usunąć przepisu: ${(e as Error).message}`);
      setDeleting(false);
    }
  };

  return (
    <div className="modal-bg" onClick={onClose}>
      <div className="recipe-detail card paper-grain" onClick={(e) => e.stopPropagation()}>
        <div style={{ position: 'relative' }}>
          {(() => {
            const imgSrc = recipeImageUrl(r);
            return imgSrc ? (
              <div
                style={{
                  height: 200,
                  borderTopLeftRadius: 'var(--r-lg)',
                  borderTopRightRadius: 'var(--r-lg)',
                  overflow: 'hidden',
                  position: 'relative',
                }}
              >
                <img
                  src={imgSrc}
                  alt=""
                  style={{
                    width: '100%',
                    height: '100%',
                    objectFit: 'cover',
                    display: 'block',
                  }}
                />
              </div>
            ) : (
              <div
                style={{
                  height: 200,
                  background: `repeating-linear-gradient(135deg, oklch(0.92 0.05 ${r.hue}) 0 12px, oklch(0.95 0.04 ${r.hue}) 12px 24px)`,
                  borderTopLeftRadius: 'var(--r-lg)',
                  borderTopRightRadius: 'var(--r-lg)',
                  position: 'relative',
                }}
              >
                <div
                  style={{
                    position: 'absolute',
                    inset: 0,
                    background: `linear-gradient(180deg,transparent 50%, oklch(0.45 0.13 ${r.hue} / 0.25))`,
                  }}
                />
                <div
                  style={{
                    position: 'absolute',
                    bottom: 10,
                    left: 14,
                    fontFamily: 'var(--mono)',
                    fontSize: 10,
                    letterSpacing: '.1em',
                    textTransform: 'uppercase',
                    color: `oklch(0.32 0.10 ${r.hue})`,
                    opacity: 0.7,
                  }}
                >
                  brak zdjęcia
                </div>
              </div>
            );
          })()}
          <input
            ref={fileInputRef}
            type="file"
            accept="image/jpeg,image/png,image/webp"
            style={{ display: 'none' }}
            onChange={onImageChosen}
          />
          <div style={{ position: 'absolute', top: 14, left: 14, display: 'flex', gap: 6 }}>
            <button className="btn" onClick={onPickImage} disabled={imageBusy}>
              {imageBusy ? 'Przesyłam…' : r.image_filename ? 'Zmień zdjęcie' : 'Dodaj zdjęcie'}
            </button>
            {r.image_filename && (
              <button className="btn" onClick={onRemoveImage} disabled={imageBusy}>
                Usuń zdjęcie
              </button>
            )}
          </div>
          <div style={{ position: 'absolute', top: 14, right: 14, display: 'flex', gap: 6 }}>
            <button
              className="btn"
              onClick={() => onToggleFavorite(baseR.id)}
              title={isFavorite ? 'Usuń z ulubionych' : 'Dodaj do ulubionych'}
              style={{ color: isFavorite ? 'oklch(0.55 0.22 15)' : undefined }}
            >
              <Icon name="heart" size={14} filled={isFavorite} />
              {isFavorite ? ' Ulubiony' : ' Dodaj do ulubionych'}
            </button>
            {!editing && baseR.created_by === currentUserId && (
              <button
                className="btn"
                disabled={ownershipBusy}
                onClick={async () => {
                  const isHousehold = baseR.owner_household_id != null;
                  setOwnershipBusy(true);
                  try {
                    const updated = await updateRecipeOwnership(baseR.id, !isHousehold);
                    Object.assign(baseR, updated);
                    setDraft((d) => (d ? { ...d, ...updated } : d));
                    emitRecipesChanged();
                  } catch (err) {
                    alert(`Nie udało się zmienić widoczności: ${(err as Error).message}`);
                  } finally {
                    setOwnershipBusy(false);
                  }
                }}
                title={
                  baseR.owner_household_id != null
                    ? 'Aktualnie widoczny dla całej grupy domowej — kliknij, aby zrobić prywatnym'
                    : 'Aktualnie prywatny — kliknij, aby udostępnić grupie domowej'
                }
              >
                <Icon name="users" size={13} />{' '}
                {ownershipBusy
                  ? '…'
                  : baseR.owner_household_id != null
                    ? 'Zrób prywatnym'
                    : 'Udostępnij grupie domowej'}
              </button>
            )}
            {!editing && (
              <button className="btn" onClick={() => setEditing(true)}>
                Edytuj
              </button>
            )}
            {!editing && (
              <button className="btn" onClick={remove} disabled={deleting} title="Usuń przepis">
                {deleting ? (
                  'Usuwam…'
                ) : (
                  <>
                    <Icon name="x" size={13} /> Usuń
                  </>
                )}
              </button>
            )}
            {editing && (
              <>
                <button className="btn" onClick={cancel} disabled={saving}>
                  Anuluj
                </button>
                <button className="btn primary" onClick={save} disabled={saving}>
                  {saving ? (
                    'Zapisywanie…'
                  ) : (
                    <>
                      <Icon name="check" size={13} /> Zapisz
                    </>
                  )}
                </button>
              </>
            )}
            <button className="btn icon" onClick={onClose}>
              <Icon name="x" size={14} />
            </button>
          </div>
        </div>
        <div style={{ padding: '24px 28px 28px' }}>
          {editing ? (
            <>
              <div style={{ marginBottom: 10 }}>
                <div className="eyebrow" style={{ marginBottom: 5 }}>
                  Tagi
                </div>
                <TagInput
                  value={draft.tags || []}
                  onChange={(v) => setDraft((d) => (d ? { ...d, tags: v } : d))}
                  suggestions={existingTags}
                />
              </div>
              <div style={{ marginBottom: 10 }}>
                <div className="eyebrow" style={{ marginBottom: 5 }}>
                  Typ posiłku
                </div>
                <MealTypePicker
                  value={draft.meal_types || []}
                  onChange={(v) => setDraft((d) => (d ? { ...d, meal_types: v } : d))}
                  suggestions={mealTypeSuggestions}
                />
              </div>
            </>
          ) : (
            <div style={{ display: 'flex', gap: 6, marginBottom: 8, flexWrap: 'wrap' }}>
              {r.tags.map((t) => (
                <span key={t} className="chip">
                  {t}
                </span>
              ))}
              {(r.meal_types || []).map((m) => (
                <span
                  key={`mt-${m}`}
                  className="chip"
                  style={{
                    background: 'var(--accent)',
                    color: 'oklch(0.98 0.015 80)',
                    borderColor: 'var(--accent-deep)',
                  }}
                >
                  {m}
                </span>
              ))}
            </div>
          )}
          {saveError && (
            <div
              style={{
                color: 'var(--terra)',
                fontSize: 13,
                padding: '6px 10px',
                background: 'oklch(0.97 0.02 15)',
                borderRadius: 'var(--r)',
                border: '1px solid oklch(0.88 0.04 15)',
                marginBottom: 10,
              }}
            >
              Nie udało się zapisać: {saveError}
            </div>
          )}
          {editing ? (
            <input
              value={draft.title}
              onChange={(e) =>
                setDraft((d) => (d ? { ...d, title: e.target.value } : d))
              }
              className="serif edit-title"
              style={{ fontStyle: 'italic', fontSize: 30, width: '100%' }}
            />
          ) : (
            <h1 className="serif" style={{ fontStyle: 'italic', fontSize: 30 }}>
              {r.title}
            </h1>
          )}
          <div
            style={{
              display: 'flex',
              gap: 18,
              marginTop: 10,
              color: 'var(--ink-3)',
              fontSize: 13,
              flexWrap: 'wrap',
              alignItems: 'center',
            }}
          >
            <span>
              <Icon name="clock" size={13} />{' '}
              {editing ? (
                <>
                  przygotowanie{' '}
                  <input
                    className="edit-num"
                    type="number"
                    value={draft.prep_time}
                    onChange={(e) =>
                      setDraft((d) => (d ? { ...d, prep_time: +e.target.value } : d))
                    }
                  />{' '}
                  + gotowanie{' '}
                  <input
                    className="edit-num"
                    type="number"
                    value={draft.cook_time}
                    onChange={(e) =>
                      setDraft((d) => (d ? { ...d, cook_time: +e.target.value } : d))
                    }
                  />
                </>
              ) : (
                <>
                  przygotowanie <span className="mono">{r.prep_time}</span> + gotowanie{' '}
                  <span className="mono">{r.cook_time}</span>
                </>
              )}{' '}
              min
            </span>
            <span>
              <Icon name="users" size={13} />{' '}
              {editing ? (
                <input
                  className="edit-num"
                  type="number"
                  value={draft.servings}
                  onChange={(e) =>
                    setDraft((d) => (d ? { ...d, servings: +e.target.value } : d))
                  }
                />
              ) : (
                <span className="mono">{r.servings}</span>
              )}{' '}
              porcji
            </span>
          </div>

          <div className="rd-grid">
            <div>
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'baseline',
                  marginBottom: 10,
                }}
              >
                <h3 className="serif" style={{ fontStyle: 'italic' }}>
                  Składniki
                </h3>
                {editing && (
                  <button
                    className="btn ghost"
                    style={{ fontSize: 11, padding: '3px 8px' }}
                    onClick={addIng}
                  >
                    <Icon name="plus" size={11} /> dodaj
                  </button>
                )}
              </div>
              <ul className="rd-ingredients">
                {r.ingredients.map((ing, i) => (
                  <li key={i}>
                    {editing ? (
                      <>
                        <input
                          className="edit-num"
                          style={{ width: 54 }}
                          type="number"
                          step="0.1"
                          value={draft.ingredients[i].qty}
                          onChange={(e) => updateIng(i, { qty: +e.target.value })}
                        />
                        <select
                          className="edit-input"
                          style={{ width: 80 }}
                          value={draft.ingredients[i].unit}
                          onChange={(e) => updateIng(i, { unit: e.target.value })}
                        >
                          {INGREDIENT_UNITS.map((u) => (
                            <option key={u} value={u}>
                              {u}
                            </option>
                          ))}
                        </select>
                        <input
                          className="edit-input"
                          style={{ flex: 1 }}
                          value={draft.ingredients[i].name}
                          onChange={(e) => updateIng(i, { name: e.target.value })}
                        />
                        <button
                          className="btn ghost icon"
                          style={{ padding: 2, marginLeft: 'auto' }}
                          onClick={() => removeIng(i)}
                        >
                          <Icon name="x" size={12} />
                        </button>
                      </>
                    ) : (
                      <>
                        <span
                          className="mono"
                          style={{ color: 'var(--ink-2)', minWidth: 64, display: 'inline-block' }}
                        >
                          {ing.qty} {ing.unit}
                        </span>
                        <span>{ing.name}</span>
                      </>
                    )}
                  </li>
                ))}
              </ul>
            </div>
            <div>
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'baseline',
                  marginBottom: 10,
                }}
              >
                <h3 className="serif" style={{ fontStyle: 'italic' }}>
                  Kroki
                </h3>
                {editing && (
                  <button
                    className="btn ghost"
                    style={{ fontSize: 11, padding: '3px 8px' }}
                    onClick={addStep}
                  >
                    <Icon name="plus" size={11} /> dodaj
                  </button>
                )}
              </div>
              <ol className="rd-steps">
                {r.steps.map((s, i) => (
                  <li key={i}>
                    <span className="rd-step-n serif">{i + 1}</span>
                    {editing ? (
                      <>
                        <textarea
                          className="edit-input"
                          style={{ flex: 1, minHeight: 50, resize: 'vertical' }}
                          value={draft.steps[i]}
                          onChange={(e) => updateStep(i, e.target.value)}
                        />
                        <button
                          className="btn ghost icon"
                          style={{ padding: 2, alignSelf: 'flex-start' }}
                          onClick={() => removeStep(i)}
                        >
                          <Icon name="x" size={12} />
                        </button>
                      </>
                    ) : (
                      <span>{s}</span>
                    )}
                  </li>
                ))}
              </ol>
            </div>
          </div>

          <div
            style={{
              marginTop: 24,
              padding: '16px 18px',
              background: 'var(--paper-2)',
              border: '1px dashed var(--line)',
              borderRadius: 'var(--r)',
            }}
          >
            <div className="eyebrow" style={{ marginBottom: 10 }}>
              Makroskładniki / porcję
            </div>
            {editing ? (
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5,1fr)', gap: 10 }}>
                <LabelField label="Kcal">
                  <input
                    className="edit-input"
                    type="number"
                    min="0"
                    value={draft.kcal}
                    onChange={(e) => setDraft((d) => (d ? { ...d, kcal: +e.target.value || 0 } : d))}
                  />
                </LabelField>
                <LabelField label="Białko (g)">
                  <input
                    className="edit-input"
                    type="number"
                    min="0"
                    value={draft.p}
                    onChange={(e) => setDraft((d) => (d ? { ...d, p: +e.target.value || 0 } : d))}
                  />
                </LabelField>
                <LabelField label="Tłuszcz (g)">
                  <input
                    className="edit-input"
                    type="number"
                    min="0"
                    value={draft.f}
                    onChange={(e) => setDraft((d) => (d ? { ...d, f: +e.target.value || 0 } : d))}
                  />
                </LabelField>
                <LabelField label="Węglo (g)">
                  <input
                    className="edit-input"
                    type="number"
                    min="0"
                    value={draft.c}
                    onChange={(e) => setDraft((d) => (d ? { ...d, c: +e.target.value || 0 } : d))}
                  />
                </LabelField>
                <LabelField label=" ">
                  <button
                    className="btn"
                    style={{ width: '100%', height: '100%' }}
                    disabled={estimating || (draft.ingredients?.length ?? 0) === 0}
                    title={(draft.ingredients?.length ?? 0) === 0 ? 'Dodaj składniki przed szacowaniem' : 'Oszacuj makra przez AI'}
                    onClick={async () => {
                      if (!draft) return;
                      setEstimating(true);
                      setSaveError(null);
                      try {
                        const est = await estimateMacros({
                          title: draft.title || 'Przepis',
                          servings: draft.servings,
                          ingredients: draft.ingredients,
                        });
                        setDraft((d) => d ? { ...d, kcal: Math.round(est.kcal), p: Math.round(est.p), f: Math.round(est.f), c: Math.round(est.c) } : d);
                      } catch (e) {
                        setSaveError('Szacowanie makr: ' + (e as Error).message);
                      } finally {
                        setEstimating(false);
                      }
                    }}
                  >
                    {estimating ? '…' : 'Szacuj AI'}
                  </button>
                </LabelField>
              </div>
            ) : (
              <Macro
                kcal={Math.round(r.kcal / r.servings)}
                p={Math.round(r.p / r.servings)}
                f={Math.round(r.f / r.servings)}
                c={Math.round(r.c / r.servings)}
                variant="donut"
              />
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
