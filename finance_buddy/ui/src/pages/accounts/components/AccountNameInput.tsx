// Pole zmiany nazwy w miejscu: Enter i wyjście z pola zapisują, Esc anuluje.
import { useRef, useState } from 'react';
import { toast } from 'sonner';
import { copy } from '../../../copy/pl';

const labels = copy.accounts;

type AccountNameInputProps = {
  name: string;
  className?: string;
  onSave: (name: string) => void;
  onCancel: () => void;
  // Sklepy korzystają z tego samego pola, z własnymi tekstami
  ariaLabel?: string;
  tooShortMessage?: string;
};

const AccountNameInput = ({ name, className, onSave, onCancel, ariaLabel, tooShortMessage }: AccountNameInputProps) => {
  const [value, setValue] = useState(name);
  // Esc i Enter kończą edycję; blur, który po nich przychodzi, nie może zapisać drugi raz.
  const settledRef = useRef(false);

  const commit = () => {
    if (settledRef.current) return;
    const next = value.trim();
    if (next === name) {
      settledRef.current = true;
      onCancel();
      return;
    }
    if (next.length < 2) {
      toast.error(tooShortMessage ?? labels.nameTooShort);
      return;
    }
    settledRef.current = true;
    onSave(next);
  };

  return (
    <input
      className={className}
      aria-label={ariaLabel ?? labels.rename(name)}
      title={labels.renameHint}
      autoFocus
      value={value}
      onFocus={(event) => event.currentTarget.select()}
      onChange={(event) => setValue(event.target.value)}
      onClick={(event) => event.stopPropagation()}
      onDoubleClick={(event) => event.stopPropagation()}
      onBlur={commit}
      onKeyDown={(event) => {
        event.stopPropagation();
        if (event.key === 'Enter') commit();
        if (event.key === 'Escape') {
          settledRef.current = true;
          onCancel();
        }
      }}
    />
  );
};

export default AccountNameInput;
