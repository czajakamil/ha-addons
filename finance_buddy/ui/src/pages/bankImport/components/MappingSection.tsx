import type { BankParseOptions } from '../../../api';
import type { AmountMode, Column, ColumnRole, Mapping } from '../mapping';

const ROLE_LABELS: Record<ColumnRole, string> = {
  ignore: '— pomiń —',
  date: 'Data',
  amount: 'Kwota (ze znakiem)',
  credit: 'Uznanie (wpływ)',
  debit: 'Obciążenie (wydatek)',
  payee: 'Kontrahent',
  memo: 'Opis / tytuł',
  currency: 'Waluta',
  fee: 'Opłata',
  status: 'Status',
  external_id: 'Nr transakcji'
};

const ENCODINGS = [
  { value: 'utf-8', label: 'UTF-8' },
  { value: 'cp1250', label: 'Windows-1250 (cp1250)' },
  { value: 'iso-8859-2', label: 'ISO-8859-2' }
];
const DELIMITERS = [
  { value: ';', label: 'średnik ;' },
  { value: ',', label: 'przecinek ,' },
  { value: '\t', label: 'tabulator' },
  { value: '|', label: 'kreska |' }
];
const DATE_FORMATS = [
  { value: '%Y-%m-%d', label: 'RRRR-MM-DD' },
  { value: '%d.%m.%Y', label: 'DD.MM.RRRR' },
  { value: '%d-%m-%Y', label: 'DD-MM-RRRR' },
  { value: '%d/%m/%Y', label: 'DD/MM/RRRR' },
  { value: '%m/%d/%Y', label: 'MM/DD/RRRR' },
  { value: '%Y/%m/%d', label: 'RRRR/MM/DD' },
  { value: '%Y.%m.%d', label: 'RRRR.MM.DD' }
];
const AMOUNT_MODES: { value: AmountMode; label: string }[] = [
  { value: 'signed', label: 'Jedna kolumna, wydatek z minusem' },
  { value: 'signed_inverted', label: 'Jedna kolumna, wydatek bez minusa' },
  { value: 'split', label: 'Osobne kolumny uznań i obciążeń' }
];

type Props = {
  columns: Column[];
  mapping: Mapping;
  /** Ustawienia nadpisane przez użytkownika (puste pole = wykryj). */
  overrides: BankParseOptions;
  /** Ustawienia, które backend faktycznie zastosował (wykryte albo nadpisane). */
  detected: BankParseOptions;
  mismatched: number[];
  errors: string[];
  onMappingChange: (mapping: Mapping) => void;
  /** Kodowanie, separator i nagłówek zmieniają kolumny — wymagają ponownego odczytu. */
  onFileOptionsChange: (options: BankParseOptions) => void;
  /** Format daty i separator dziesiętny działają już na zmapowanych kolumnach. */
  onValueOptionsChange: (options: BankParseOptions) => void;
};

const MappingSection = ({
  columns,
  mapping,
  overrides,
  detected,
  mismatched,
  errors,
  onMappingChange,
  onFileOptionsChange,
  onValueOptionsChange
}: Props) => {
  const setRole = (index: number, role: ColumnRole) => {
    const next = { ...mapping.columns };
    if (role === 'ignore') delete next[String(index)];
    else next[String(index)] = role;
    onMappingChange({ ...mapping, columns: next });
  };

  const hasStatus = Object.values(mapping.columns).includes('status');
  const detectedLabel = (value: string | number | null | undefined, list?: { value: string; label: string }[]) => {
    if (value === null || value === undefined) return 'nie wykryto';
    return list?.find((o) => o.value === value)?.label ?? String(value);
  };

  return (
    <div className="bank-import__mapping">
      <details className="bank-import__settings">
        <summary>Ustawienia pliku</summary>
        <div className="bank-import__settings-grid">
          <label className="form-label">
            Kodowanie
            <select
              className="form-select form-select-sm"
              value={overrides.encoding ?? ''}
              onChange={(e) =>
                onFileOptionsChange({ ...overrides, encoding: e.target.value || null })
              }
            >
              <option value="">Wykryj ({detectedLabel(detected.encoding, ENCODINGS)})</option>
              {ENCODINGS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </label>
          <label className="form-label">
            Separator
            <select
              className="form-select form-select-sm"
              value={overrides.delimiter ?? ''}
              onChange={(e) =>
                onFileOptionsChange({ ...overrides, delimiter: e.target.value || null })
              }
            >
              <option value="">Wykryj ({detectedLabel(detected.delimiter, DELIMITERS)})</option>
              {DELIMITERS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </label>
          <label className="form-label">
            Wiersz nagłówka
            <input
              type="number"
              min={0}
              className="form-control form-control-sm"
              placeholder={`Wykryj (${
                detected.header_row == null || detected.header_row < 0
                  ? 'brak'
                  : detected.header_row + 1
              })`}
              value={overrides.header_row == null ? '' : overrides.header_row + 1}
              onChange={(e) => {
                const raw = e.target.value;
                onFileOptionsChange({
                  ...overrides,
                  // 0 w polu = plik bez nagłówka (-1 po stronie serwera).
                  header_row: raw === '' ? null : Math.max(-1, Number(raw) - 1)
                });
              }}
            />
          </label>
          <label className="form-label">
            Format daty
            <select
              className="form-select form-select-sm"
              value={overrides.date_format ?? ''}
              onChange={(e) =>
                onValueOptionsChange({ ...overrides, date_format: e.target.value || null })
              }
            >
              <option value="">Wykryj ({detectedLabel(detected.date_format, DATE_FORMATS)})</option>
              {DATE_FORMATS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </label>
          <label className="form-label">
            Separator dziesiętny
            <select
              className="form-select form-select-sm"
              value={overrides.decimal_separator ?? ''}
              onChange={(e) =>
                onValueOptionsChange({
                  ...overrides,
                  decimal_separator: (e.target.value || null) as ',' | '.' | null
                })
              }
            >
              <option value="">Wykryj ({detected.decimal_separator ?? 'nie wykryto'})</option>
              <option value=",">przecinek 12,34</option>
              <option value=".">kropka 12.34</option>
            </select>
          </label>
          <label className="form-label">
            Znak kwoty
            <select
              className="form-select form-select-sm"
              value={mapping.amount_mode}
              onChange={(e) =>
                onMappingChange({ ...mapping, amount_mode: e.target.value as AmountMode })
              }
            >
              {AMOUNT_MODES.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </label>
          {hasStatus ? (
            <label className="form-label">
              Importuj tylko statusy
              <input
                className="form-control form-control-sm"
                placeholder="np. COMPLETED (puste = wszystkie)"
                value={mapping.status_accepted.join(', ')}
                onChange={(e) =>
                  onMappingChange({
                    ...mapping,
                    status_accepted: e.target.value
                      .split(',')
                      .map((s) => s.trim().toUpperCase())
                      .filter(Boolean)
                  })
                }
              />
            </label>
          ) : null}
        </div>
      </details>

      <div className="table-responsive mt-2">
        <table className="table table-sm table-vcenter bank-import__columns">
          <thead>
            <tr>
              <th>Kolumna w pliku</th>
              <th>Przykładowe wartości</th>
              <th style={{ width: '14rem' }}>To jest…</th>
            </tr>
          </thead>
          <tbody>
            {columns.map((column) => {
              const role = mapping.columns[String(column.index)] ?? 'ignore';
              const moved = mismatched.includes(column.index);
              return (
                <tr key={column.index} className={role !== 'ignore' ? 'is-mapped' : undefined}>
                  <td>
                    <span className="text-secondary me-1">{column.index + 1}.</span>
                    {column.name || <span className="text-secondary">(bez nazwy)</span>}
                    {moved ? (
                      <span
                        className="receipt-badge receipt-badge--warn ms-2"
                        title="W zapisanym profilu ta kolumna miała inną nazwę — sprawdź rolę."
                      >
                        zmieniona
                      </span>
                    ) : null}
                  </td>
                  <td className="bank-import__samples">
                    {column.samples.length ? (
                      column.samples.slice(0, 3).join(' · ')
                    ) : (
                      <span className="text-secondary">pusta</span>
                    )}
                  </td>
                  <td>
                    <select
                      className="form-select form-select-sm"
                      aria-label={`Rola kolumny ${column.name || column.index + 1}`}
                      value={role}
                      onChange={(e) => setRole(column.index, e.target.value as ColumnRole)}
                    >
                      {(Object.keys(ROLE_LABELS) as ColumnRole[]).map((r) => (
                        <option key={r} value={r}>
                          {ROLE_LABELS[r]}
                        </option>
                      ))}
                    </select>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {errors.length ? (
        <div className="alert alert-warning mb-0" role="alert">
          {errors.map((e) => (
            <div key={e}>{e}</div>
          ))}
        </div>
      ) : null}
    </div>
  );
};

export default MappingSection;
