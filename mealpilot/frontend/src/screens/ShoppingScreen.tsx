import { useEffect, useRef, useState } from 'react';
import { Icon } from '../components/Icon';
import {
  addShoppingItem,
  categoryOf,
  clearShopping,
  deleteShoppingItem,
  emitShoppingChanged,
  getShopping,
  loadShopping,
  PLAN_CHANGED,
  RECIPES_CHANGED,
  regenerateShopping,
  setShoppingChecked,
  SHOPPING_CHANGED,
  WEEK_START,
} from '../data';
import type { ShoppingItem } from '../types';

const CATEGORIES = [
  'Mięso, ryby, białko',
  'Nabiał',
  'Warzywa i owoce',
  'Suche i zboża',
  'Tłuszcze i przyprawy',
  'Spiżarnia',
  'Inne',
] as const;

const CUSTOM_UNITS = ['szt', 'g', 'kg', 'ml', 'l', 'opak'] as const;

export function ShoppingScreen() {
  const ws = WEEK_START;
  const [items, setItems] = useState<ShoppingItem[]>(() => getShopping(ws));
  const [loading, setLoading] = useState(items.length === 0);
  const [regenerating, setRegenerating] = useState(false);
  const [newItem, setNewItem] = useState({ name: '', qty: '', unit: 'szt', cat: 'Inne' });
  const [adding, setAdding] = useState(false);
  const [settled, setSettled] = useState<Record<number, boolean>>({});
  const timers = useRef<Record<number, ReturnType<typeof setTimeout>>>({});

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const loaded = await loadShopping(ws);
        if (cancelled) return;
        if (loaded.length === 0) {
          const generated = await regenerateShopping(ws);
          if (!cancelled) setItems([...generated]);
        } else {
          setItems([...loaded]);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [ws]);

  useEffect(() => {
    const sync = () => setItems([...getShopping(ws)]);
    const reloadFromBackend = () => {
      void loadShopping(ws).then(sync);
    };
    window.addEventListener(SHOPPING_CHANGED, sync);
    window.addEventListener(PLAN_CHANGED, reloadFromBackend);
    window.addEventListener(RECIPES_CHANGED, reloadFromBackend);
    return () => {
      window.removeEventListener(SHOPPING_CHANGED, sync);
      window.removeEventListener(PLAN_CHANGED, reloadFromBackend);
      window.removeEventListener(RECIPES_CHANGED, reloadFromBackend);
    };
  }, [ws]);

  const grouped: Record<string, ShoppingItem[]> = {};
  items.forEach((it) => {
    grouped[it.category] ||= [];
    grouped[it.category].push(it);
  });
  const cats = CATEGORIES.filter((c) => grouped[c]);
  const customCount = items.filter((it) => it.is_custom).length;
  const totalItems = items.length;
  const checkedCount = items.filter((it) => it.checked).length;
  const fmt = (q: number) => (Number.isInteger(q) ? q : Math.round(q * 10) / 10);

  const toggle = async (item: ShoppingItem) => {
    const nowChecked = !item.checked;
    setItems((prev) => prev.map((it) => (it.id === item.id ? { ...it, checked: nowChecked } : it)));
    if (nowChecked) {
      timers.current[item.id] = setTimeout(() => {
        setSettled((s) => ({ ...s, [item.id]: true }));
        delete timers.current[item.id];
      }, 3000);
    } else {
      if (timers.current[item.id]) {
        clearTimeout(timers.current[item.id]);
        delete timers.current[item.id];
      }
      setSettled((s) => {
        const next = { ...s };
        delete next[item.id];
        return next;
      });
    }
    try {
      await setShoppingChecked(ws, item.id, nowChecked);
      emitShoppingChanged();
    } catch {
      setItems((prev) => prev.map((it) => (it.id === item.id ? { ...it, checked: !nowChecked } : it)));
    }
  };

  const submitCustom = async () => {
    const name = newItem.name.trim();
    if (!name) return;
    const qty = parseFloat(newItem.qty) || 1;
    const cat = categoryOf(name) !== 'Inne' ? categoryOf(name) : newItem.cat;
    try {
      await addShoppingItem(ws, { name, qty, unit: newItem.unit, category: cat });
      setItems([...getShopping(ws)]);
      emitShoppingChanged();
    } catch {
      /* ignore — UI stays consistent with state */
    }
    setNewItem({ name: '', qty: '', unit: 'szt', cat: 'Inne' });
    setAdding(false);
  };

  const removeItem = async (item: ShoppingItem) => {
    setItems((prev) => prev.filter((it) => it.id !== item.id));
    try {
      await deleteShoppingItem(ws, item.id);
      emitShoppingChanged();
    } catch {
      setItems([...getShopping(ws)]);
    }
  };

  const clearAll = async () => {
    if (!window.confirm('Wyczyścić całą listę zakupów? Tej operacji nie można cofnąć.')) return;
    setItems([]);
    try {
      await clearShopping(ws);
      emitShoppingChanged();
    } catch {
      setItems([...getShopping(ws)]);
    }
  };

  const [exportOpen, setExportOpen] = useState(false);
  const exportRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!exportOpen) return;
    const close = (e: MouseEvent) => {
      if (exportRef.current && !exportRef.current.contains(e.target as Node)) {
        setExportOpen(false);
      }
    };
    document.addEventListener('mousedown', close);
    return () => document.removeEventListener('mousedown', close);
  }, [exportOpen]);

  const exportMarkdown = () => {
    setExportOpen(false);
    const lines: string[] = ['# Lista zakupów\n'];
    cats.forEach((cat) => {
      lines.push(`## ${cat}\n`);
      grouped[cat].forEach((it) => {
        const check = it.checked ? '[x]' : '[ ]';
        lines.push(`- ${check} ${it.name} — ${fmt(it.qty)} ${it.unit}`);
      });
      lines.push('');
    });
    const blob = new Blob([lines.join('\n')], { type: 'text/markdown' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'lista-zakupow.md';
    a.click();
    URL.revokeObjectURL(a.href);
  };

  const exportPdf = () => {
    setExportOpen(false);
    const html = `<!DOCTYPE html>
<html lang="pl">
<head>
  <meta charset="utf-8"/>
  <title>Lista zakupów</title>
  <style>
    body { font-family: Georgia, serif; max-width: 700px; margin: 40px auto; color: #222; }
    h1 { font-size: 28px; margin-bottom: 4px; }
    .sub { font-size: 13px; color: #888; margin-bottom: 32px; }
    h2 { font-size: 16px; border-bottom: 1px solid #ddd; padding-bottom: 4px; margin-top: 24px; margin-bottom: 10px; }
    ul { list-style: none; padding: 0; margin: 0; }
    li { display: flex; justify-content: space-between; padding: 5px 0; border-bottom: 1px dotted #eee; font-size: 14px; }
    li.checked { color: #aaa; text-decoration: line-through; }
    .qty { font-family: monospace; font-size: 13px; color: #555; }
    @media print { body { margin: 20px; } }
  </style>
</head>
<body>
  <h1>Lista zakupów</h1>
  <div class="sub">${totalItems} pozycji · ${checkedCount} odhaczone</div>
  ${cats
    .map(
      (cat) => `<h2>${cat}</h2><ul>${grouped[cat]
        .map(
          (it) =>
            `<li class="${it.checked ? 'checked' : ''}"><span>${it.name}</span><span class="qty">${fmt(it.qty)} ${it.unit}</span></li>`,
        )
        .join('')}</ul>`,
    )
    .join('')}
</body>
</html>`;
    const win = window.open('', '_blank');
    if (!win) return;
    win.document.write(html);
    win.document.close();
    win.focus();
    win.print();
  };

  const refreshFromPlan = async () => {
    setRegenerating(true);
    try {
      const generated = await regenerateShopping(ws);
      setItems([...generated]);
      emitShoppingChanged();
    } finally {
      setRegenerating(false);
    }
  };

  return (
    <div>
      <div className="page-head">
        <div>
          <div className="eyebrow">Tydzień 4 maj – 10 maj</div>
          <h1 className="serif" style={{ fontStyle: 'italic' }}>
            Lista zakupów
          </h1>
          <div className="sub">
            {totalItems} pozycji ·{' '}
            {customCount > 0 && <>w tym {customCount} własnych · </>}
            {checkedCount}/{totalItems} odhaczone
            {loading && ' · ładowanie…'}
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button className="btn" onClick={() => setAdding(true)}>
            <Icon name="plus" size={14} /> Dodaj rzecz
          </button>
          <button className="btn" onClick={refreshFromPlan} disabled={regenerating}>
            <Icon name="spark" size={14} /> {regenerating ? 'Odświeżam…' : 'Zaczytaj z planu przepisów'}
          </button>
          <button className="btn" onClick={() => void clearAll()} disabled={items.length === 0}>
            <Icon name="x" size={14} /> Wyczyść
          </button>
          <div ref={exportRef} style={{ position: 'relative' }}>
            <button className="btn" onClick={() => setExportOpen((v) => !v)}>
              Eksport <span style={{ fontSize: 10, marginLeft: 4, opacity: 0.6 }}>▾</span>
            </button>
            {exportOpen && (
              <div
                style={{
                  position: 'absolute',
                  top: 'calc(100% + 6px)',
                  right: 0,
                  background: 'var(--card)',
                  border: '1px solid var(--line)',
                  borderRadius: 'var(--r)',
                  boxShadow: 'var(--shadow)',
                  minWidth: 160,
                  zIndex: 99,
                  overflow: 'hidden',
                }}
              >
                <button
                  className="btn ghost"
                  style={{ width: '100%', justifyContent: 'flex-start', borderRadius: 0, padding: '10px 14px' }}
                  onClick={exportMarkdown}
                >
                  Markdown <span className="mono" style={{ fontSize: 10, marginLeft: 6, opacity: 0.5 }}>.md</span>
                </button>
                <button
                  className="btn ghost"
                  style={{ width: '100%', justifyContent: 'flex-start', borderRadius: 0, padding: '10px 14px', borderTop: '1px solid var(--line-soft)' }}
                  onClick={exportPdf}
                >
                  PDF <span className="mono" style={{ fontSize: 10, marginLeft: 6, opacity: 0.5 }}>.pdf</span>
                </button>
              </div>
            )}
          </div>
        </div>
      </div>

      <div
        style={{
          display: 'flex',
          gap: 6,
          height: 8,
          borderRadius: 99,
          overflow: 'hidden',
          background: 'var(--line-soft)',
          marginBottom: 24,
        }}
      >
        <div
          style={{
            width: `${totalItems ? (checkedCount / totalItems) * 100 : 0}%`,
            background: 'var(--olive)',
            transition: 'width .3s',
          }}
        />
      </div>

      <div className="shop-grid">
        {cats.map((cat) => (
          <div key={cat} className="card paper-grain" style={{ padding: '16px 18px' }}>
            <div
              style={{
                display: 'flex',
                alignItems: 'baseline',
                justifyContent: 'space-between',
                marginBottom: 10,
              }}
            >
              <h3 className="serif" style={{ fontStyle: 'italic' }}>
                {cat}
              </h3>
              <span className="mono" style={{ fontSize: 11, color: 'var(--ink-faint)' }}>
                {grouped[cat].length}
              </span>
            </div>
            <ul className="shop-list">
              {[...grouped[cat]]
                .sort((a, b) => (settled[a.id] ? 1 : 0) - (settled[b.id] ? 1 : 0))
                .map((it) => (
                  <li
                    key={it.id}
                    className="shop-item"
                    data-checked={it.checked}
                    onClick={() => void toggle(it)}
                  >
                    <span className="shop-check">
                      {it.checked && <Icon name="check" size={11} />}
                    </span>
                    <span className="shop-name">
                      {it.name}
                      {it.is_custom && (
                        <span
                          className="chip"
                          style={{
                            marginLeft: 6,
                            fontSize: 9,
                            padding: '0 6px',
                            background: 'var(--butter)',
                            color: 'oklch(0.42 0.07 75)',
                          }}
                        >
                          własne
                        </span>
                      )}
                    </span>
                    <span className="mono shop-qty">
                      {fmt(it.qty)} {it.unit}
                    </span>
                    {it.is_custom && (
                      <button
                        className="btn ghost icon"
                        style={{ padding: 2, marginLeft: 4 }}
                        onClick={(e) => {
                          e.stopPropagation();
                          void removeItem(it);
                        }}
                      >
                        <Icon name="x" size={11} />
                      </button>
                    )}
                  </li>
                ))}
            </ul>
          </div>
        ))}
      </div>

      {adding && (
        <div className="modal-bg" onClick={() => setAdding(false)}>
          <div
            className="modal card"
            style={{ maxWidth: 440 }}
            onClick={(e) => e.stopPropagation()}
          >
            <div className="modal-head">
              <div>
                <div className="eyebrow">Lista zakupów</div>
                <h2 className="serif" style={{ fontStyle: 'italic' }}>
                  Dodaj własną pozycję
                </h2>
              </div>
              <button className="btn ghost icon" onClick={() => setAdding(false)}>
                <Icon name="x" size={16} />
              </button>
            </div>
            <div
              style={{
                padding: '6px 18px 18px',
                display: 'flex',
                flexDirection: 'column',
                gap: 12,
              }}
            >
              <label style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
                <span className="eyebrow">Nazwa</span>
                <input
                  autoFocus
                  className="edit-input"
                  placeholder="np. papier toaletowy"
                  value={newItem.name}
                  onChange={(e) => setNewItem((p) => ({ ...p, name: e.target.value }))}
                  onKeyDown={(e) => e.key === 'Enter' && void submitCustom()}
                />
              </label>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
                <label style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
                  <span className="eyebrow">Ilość</span>
                  <input
                    className="edit-input"
                    type="number"
                    step="0.1"
                    placeholder="1"
                    value={newItem.qty}
                    onChange={(e) => setNewItem((p) => ({ ...p, qty: e.target.value }))}
                  />
                </label>
                <label style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
                  <span className="eyebrow">Jednostka</span>
                  <select
                    className="edit-input"
                    value={newItem.unit}
                    onChange={(e) => setNewItem((p) => ({ ...p, unit: e.target.value }))}
                  >
                    {CUSTOM_UNITS.map((u) => (
                      <option key={u} value={u}>
                        {u}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
              <label style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
                <span className="eyebrow">Kategoria</span>
                <select
                  className="edit-input"
                  value={newItem.cat}
                  onChange={(e) => setNewItem((p) => ({ ...p, cat: e.target.value }))}
                >
                  {CATEGORIES.map((c) => (
                    <option key={c} value={c}>
                      {c}
                    </option>
                  ))}
                </select>
              </label>
              <div
                style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 4 }}
              >
                <button className="btn ghost" onClick={() => setAdding(false)}>
                  Anuluj
                </button>
                <button
                  className="btn primary"
                  onClick={() => void submitCustom()}
                  disabled={!newItem.name.trim()}
                >
                  Dodaj
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
