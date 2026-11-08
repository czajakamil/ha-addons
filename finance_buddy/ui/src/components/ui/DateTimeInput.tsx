// Pola daty i godziny. Natywny wybierak wywraca aplikację Home Assistant na Maca (Mac Catalyst
// rzuca wyjątek przy UIPickerView w popoverze), więc tam pole jest tekstowe.
import { useState, type InputHTMLAttributes } from 'react';
import { formatTypedDate, parseTypedDate, parseTypedTime } from '../../utils/date';

type FieldProps = Omit<InputHTMLAttributes<HTMLInputElement>, 'type' | 'value' | 'onChange'> & {
  value: string;
  onChange: (value: string) => void;
};

// Prawdziwe przeglądarki na Macu mają w user agencie Safari albo Firefox; osadzony WebView nie ma.
// iPad też podaje się za Macintosh, ale ma ekran dotykowy.
export const isMacEmbeddedWebView = (
  userAgent = navigator.userAgent,
  maxTouchPoints = navigator.maxTouchPoints
) => /Macintosh/.test(userAgent) && maxTouchPoints === 0 && !/Safari|Firefox/.test(userAgent);

type TypedInputProps = FieldProps & {
  parse: (text: string) => string | null;
  format: (value: string) => string;
  hint: string;
};

const TypedInput = ({ value, onChange, onBlur, parse, format, hint, ...rest }: TypedInputProps) => {
  const [draft, setDraft] = useState(() => format(value));
  const [synced, setSynced] = useState(value);
  // Zmiana z zewnątrz (reset formularza, inna transakcja) nadpisuje wpisywany tekst
  if (value !== synced) {
    setSynced(value);
    setDraft(format(value));
  }

  return (
    <input
      {...rest}
      type="text"
      placeholder={hint}
      value={draft}
      onChange={(event) => {
        const text = event.target.value;
        const parsed = parse(text);
        setDraft(text);
        // Niepełny tekst nie idzie dalej, a błąd blokuje wysłanie formularza
        event.target.setCustomValidity(parsed === null ? `Wpisz w formacie ${hint}` : '');
        if (parsed !== null) {
          setSynced(parsed);
          onChange(parsed);
        }
      }}
      onBlur={(event) => {
        if (parse(draft) !== null) setDraft(format(parse(draft) ?? ''));
        onBlur?.(event);
      }}
    />
  );
};

export const DateInput = ({ value, onChange, ...rest }: FieldProps) =>
  isMacEmbeddedWebView() ? (
    <TypedInput {...rest} value={value} onChange={onChange} parse={parseTypedDate} format={formatTypedDate} hint="DD.MM.RRRR" />
  ) : (
    <input {...rest} type="date" value={value} onChange={(event) => onChange(event.target.value)} />
  );

export const TimeInput = ({ value, onChange, ...rest }: FieldProps) =>
  isMacEmbeddedWebView() ? (
    <TypedInput {...rest} value={value} onChange={onChange} parse={parseTypedTime} format={(time) => time} hint="GG:MM" />
  ) : (
    <input {...rest} type="time" value={value} onChange={(event) => onChange(event.target.value)} />
  );
