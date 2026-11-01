import { useCallback, useEffect, useLayoutEffect, useRef, useState, type CSSProperties } from 'react';
import { IconCircleOff, IconPalette } from '@tabler/icons-react';
import { TagColorDot } from './TagChip';
import {
  DEFAULT_CUSTOM_HSL,
  TAG_COLOR_OPTIONS,
  getTagColorValue,
  hexToHsl,
  hslToHex,
  isCustomTagColor,
  type Hsl,
  type ResolvedTagColor
} from './tagColors';

type TagColorPickerProps = {
  /** Efektywny kolor tagu (własny lub odziedziczony) — do podglądu na przycisku. */
  resolved: ResolvedTagColor;
  /** Czy tag ma WŁASNY kolor (do podświetlenia aktywnej próbki i opcji „Dziedzicz"). */
  ownColorId: string | null;
  /** Czy tag ma rodzica — bez niego dziedziczenie nie ma sensu (korzeń drzewa). */
  canInherit?: boolean;
  disabled?: boolean;
  /** id z palety, albo null = wyczyść własny kolor (dziedzicz po rodzicu). */
  onSelect: (colorId: string | null) => void;
};

/** Klawisze zmieniające wartość suwaka — tylko po nich commitujemy kolor przy keyup. */
const HSL_COMMIT_KEYS = ['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End', 'PageUp', 'PageDown'];

/** Przycisk-kropka otwierający paletę kolorów tagu z opcją dziedziczenia. */
const TagColorPicker = ({ resolved, ownColorId, canInherit = true, disabled, onSelect }: TagColorPickerProps) => {
  const [open, setOpen] = useState(false);
  const [customOpen, setCustomOpen] = useState(false);
  const [hsl, setHsl] = useState<Hsl>(() =>
    isCustomTagColor(ownColorId) ? hexToHsl(ownColorId) : DEFAULT_CUSTOM_HSL
  );
  const rootRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const popoverRef = useRef<HTMLDivElement>(null);
  const [coords, setCoords] = useState<{ top: number; left: number } | null>(null);

  const isCustomActive = isCustomTagColor(ownColorId);
  const customHex = hslToHex(hsl);

  // Popover jest `position: fixed` i kotwiczony do triggera, żeby wychodził spod
  // `overflow: hidden` karty (inaczej „ginie" na dole tabeli). Przy braku miejsca
  // pod spodem — odbija się nad trigger.
  const updatePosition = useCallback(() => {
    const trigger = triggerRef.current;
    if (!trigger) return;
    const rect = trigger.getBoundingClientRect();
    const width = popoverRef.current?.offsetWidth ?? 190;
    const height = popoverRef.current?.offsetHeight ?? 0;
    const margin = 4;
    const left = Math.max(8, Math.min(rect.left, window.innerWidth - width - 8));
    let top = rect.bottom + margin;
    if (height && top + height > window.innerHeight - 8) {
      const above = rect.top - margin - height;
      top = above >= 8 ? above : Math.max(8, window.innerHeight - height - 8);
    }
    setCoords({ top, left });
  }, []);

  const toggleOpen = () => {
    setOpen((prev) => {
      const next = !prev;
      if (next) {
        // Odświeżamy suwaki dopiero przy otwarciu popovera, żeby nie „skakały” w trakcie przeciągania.
        setHsl(isCustomTagColor(ownColorId) ? hexToHsl(ownColorId) : DEFAULT_CUSTOM_HSL);
        setCustomOpen(false);
      }
      return next;
    });
  };

  // Ustawiamy pozycję po wyrenderowaniu popovera (zależnie też od rozwinięcia panelu
  // niestandardowego, który zmienia wysokość) oraz nasłuchujemy scrolla/resize.
  useLayoutEffect(() => {
    if (!open) return;
    updatePosition();
    window.addEventListener('scroll', updatePosition, true);
    window.addEventListener('resize', updatePosition);
    return () => {
      window.removeEventListener('scroll', updatePosition, true);
      window.removeEventListener('resize', updatePosition);
    };
  }, [open, customOpen, updatePosition]);

  useEffect(() => {
    if (!open) return;
    const handlePointerDown = (event: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(event.target as Node)) {
        setOpen(false);
      }
    };
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setOpen(false);
    };
    document.addEventListener('mousedown', handlePointerDown);
    document.addEventListener('keydown', handleKeyDown);
    return () => {
      document.removeEventListener('mousedown', handlePointerDown);
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [open]);

  const choose = (colorId: string | null) => {
    onSelect(colorId);
    setOpen(false);
  };

  // Przeciąganie suwaka tylko aktualizuje podgląd; zapis (onSelect) idzie dopiero
  // po puszczeniu (pointerup / puszczenie klawisza) — inaczej każdy piksel ruchu
  // strzelał zapisem do API i suwaki „zacinały się" w trakcie przeciągania.
  const updateHsl = (patch: Partial<Hsl>) => {
    setHsl((prev) => ({ ...prev, ...patch }));
  };

  const commitHsl = () => {
    onSelect(hslToHex(hsl));
  };

  const commitHslOnKeyUp = ({ key }: { key: string }) => {
    if (HSL_COMMIT_KEYS.includes(key)) commitHsl();
  };

  const handleHexInput = (raw: string) => {
    const value = raw.startsWith('#') ? raw : `#${raw}`;
    if (/^#[0-9a-fA-F]{6}$/.test(value)) {
      setHsl(hexToHsl(value));
      onSelect(value);
    }
  };

  const hueTrack = 'linear-gradient(to right, #ff0000, #ffff00, #00ff00, #00ffff, #0000ff, #ff00ff, #ff0000)';
  const satTrack = `linear-gradient(to right, hsl(${hsl.h}, 0%, ${hsl.l}%), hsl(${hsl.h}, 100%, ${hsl.l}%))`;
  const lightTrack = `linear-gradient(to right, #000000, hsl(${hsl.h}, ${hsl.s}%, 50%), #ffffff)`;

  return (
    <div className="tag-color-picker" ref={rootRef}>
      <button
        ref={triggerRef}
        type="button"
        className="tag-color-picker__trigger is-focusable"
        onClick={toggleOpen}
        disabled={disabled}
        aria-haspopup="true"
        aria-expanded={open}
        title="Zmień kolor tagu"
      >
        <TagColorDot value={resolved.value} inherited={resolved.inherited} size={14} />
      </button>

      {open && (
        <div
          ref={popoverRef}
          className="tag-color-picker__popover"
          role="menu"
          style={{ top: coords?.top ?? -9999, left: coords?.left ?? -9999, visibility: coords ? 'visible' : 'hidden' }}
        >
          <div className="tag-color-picker__swatches">
            {TAG_COLOR_OPTIONS.map((option) => (
              <button
                key={option.id}
                type="button"
                className={`tag-color-picker__swatch${ownColorId === option.id ? ' is-selected' : ''}`}
                style={{ '--tag-color': option.value } as CSSProperties}
                onClick={() => choose(option.id)}
                title={option.label}
                aria-label={option.label}
              />
            ))}
            <button
              type="button"
              className={`tag-color-picker__swatch tag-color-picker__swatch--custom${isCustomActive ? ' is-selected' : ''}`}
              style={{ '--tag-color': isCustomActive ? getTagColorValue(ownColorId) : customHex } as CSSProperties}
              onClick={() => setCustomOpen((prev) => !prev)}
              title="Kolor niestandardowy"
              aria-haspopup="true"
              aria-expanded={customOpen}
              aria-label="Kolor niestandardowy"
            >
              {!isCustomActive && <IconPalette size={12} aria-hidden="true" />}
            </button>
          </div>

          {customOpen && (
            <div className="tag-color-picker__custom">
              <div className="tag-color-picker__custom-preview">
                <TagColorDot value={customHex} size={16} />
                <input
                  className="tag-color-picker__hex"
                  type="text"
                  value={customHex}
                  onChange={(event) => handleHexInput(event.target.value)}
                  maxLength={7}
                  spellCheck={false}
                  aria-label="Kod koloru (hex)"
                />
              </div>
              <label className="tag-color-picker__slider">
                <span>Odcień</span>
                <input
                  type="range"
                  min={0}
                  max={359}
                  value={hsl.h}
                  style={{ '--track': hueTrack } as CSSProperties}
                  onChange={(event) => updateHsl({ h: Number(event.target.value) })}
                  onPointerUp={commitHsl}
                  onKeyUp={commitHslOnKeyUp}
                />
              </label>
              <label className="tag-color-picker__slider">
                <span>Nasycenie</span>
                <input
                  type="range"
                  min={0}
                  max={100}
                  value={hsl.s}
                  style={{ '--track': satTrack } as CSSProperties}
                  onChange={(event) => updateHsl({ s: Number(event.target.value) })}
                  onPointerUp={commitHsl}
                  onKeyUp={commitHslOnKeyUp}
                />
              </label>
              <label className="tag-color-picker__slider">
                <span>Jasność</span>
                <input
                  type="range"
                  min={0}
                  max={100}
                  value={hsl.l}
                  style={{ '--track': lightTrack } as CSSProperties}
                  onChange={(event) => updateHsl({ l: Number(event.target.value) })}
                  onPointerUp={commitHsl}
                  onKeyUp={commitHslOnKeyUp}
                />
              </label>
            </div>
          )}
          {canInherit && (
            <button
              type="button"
              className={`tag-color-picker__inherit${ownColorId === null ? ' is-selected' : ''}`}
              onClick={() => choose(null)}
            >
              <IconCircleOff size={14} aria-hidden="true" />
              <span>Dziedzicz z rodzica</span>
            </button>
          )}
        </div>
      )}
    </div>
  );
};

export default TagColorPicker;
