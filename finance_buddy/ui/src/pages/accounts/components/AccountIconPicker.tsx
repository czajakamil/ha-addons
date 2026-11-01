// Wybór ikony i koloru awatara konta: podgląd + paleta kolorów + siatka ikon.
// Sterowany (controlled) — stan trzyma rodzic, tu tylko prezentacja i callbacki.
// Paleta i siatka są zwijane: domyślnie widać tylko podgląd, resztę rozwija się
// klikając nagłówek (mniej wizualnego szumu w formularzu konta).
import { useId, useState } from 'react';
import { ChevronDownIcon } from 'lucide-react';
import { ACCOUNT_COLOR_OPTIONS, ACCOUNT_ICON_OPTIONS } from '../accountPresentation';
import AccountAvatar from './AccountAvatar';

type AccountIconPickerProps = {
  icon: string;
  color: string;
  onIconChange: (iconId: string) => void;
  onColorChange: (colorId: string) => void;
  disabled?: boolean;
};

const AccountIconPicker = ({
  icon,
  color,
  onIconChange,
  onColorChange,
  disabled = false
}: AccountIconPickerProps) => {
  const [expanded, setExpanded] = useState(false);
  const panelId = useId();

  return (
    <div className={`account-icon-picker${expanded ? ' is-expanded' : ''}`}>
      <button
        type="button"
        className="account-icon-picker__toggle"
        aria-expanded={expanded}
        aria-controls={panelId}
        onClick={() => setExpanded((prev) => !prev)}
      >
        <span className="account-icon-picker__preview">
          <AccountAvatar icon={icon} color={color} />
          <span className="account-icon-picker__preview-label">
            {expanded ? 'Podgląd ikony konta' : 'Zmień ikonę i kolor'}
          </span>
        </span>
        <ChevronDownIcon
          className="account-icon-picker__chevron"
          size={18}
          strokeWidth={1.7}
          aria-hidden
        />
      </button>

      {expanded && (
        <div id={panelId} className="account-icon-picker__panel">
          <div
            className="account-icon-picker__colors"
            role="radiogroup"
            aria-label="Kolor ikony konta"
          >
            {ACCOUNT_COLOR_OPTIONS.map((option) => (
              <button
                key={option.id}
                type="button"
                className={`account-icon-picker__color${color === option.id ? ' is-selected' : ''}`}
                style={{ '--swatch-color': option.value } as React.CSSProperties}
                role="radio"
                aria-checked={color === option.id}
                aria-label={option.label}
                title={option.label}
                onClick={() => onColorChange(option.id)}
                disabled={disabled}
              />
            ))}
          </div>

          <div
            className="account-icon-picker__grid"
            role="radiogroup"
            aria-label="Ikona konta"
          >
            {ACCOUNT_ICON_OPTIONS.map((option) => {
              const Icon = option.Icon;
              const isSelected = icon === option.id;
              return (
                <button
                  key={option.id}
                  type="button"
                  className={`account-icon-picker__icon${isSelected ? ' is-selected' : ''}`}
                  role="radio"
                  aria-checked={isSelected}
                  aria-label={option.label}
                  title={option.label}
                  onClick={() => onIconChange(option.id)}
                  disabled={disabled}
                >
                  <Icon size={20} strokeWidth={1.7} />
                </button>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};

export default AccountIconPicker;
