import { useEffect, useMemo, useRef, useState, type ChangeEvent } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { FileSpreadsheetIcon, SettingsIcon, Undo2Icon } from 'lucide-react';
import Button from '../../components/Button';
import { copy } from '../../copy/pl';
import {
  ApiError,
  commitBankFile,
  getAccounts,
  getBankImportBatches,
  inspectBankFile,
  previewBankFile,
  undoBankImportBatch,
  type BankImportBatch,
  type BankInspectResponse,
  type BankParseOptions,
  type BankPreviewResponse
} from '../../api';
import { useApi } from '../../hooks/useApi';
import { formatMoney } from '../../utils/format';
import MappingSection from './components/MappingSection';
import PreviewSection from './components/PreviewSection';
import {
  buildDecisions,
  suggestMapping,
  summarize,
  validateMapping,
  type Mapping,
  type RowOverride
} from './mapping';

const EMPTY_MAPPING: Mapping = { columns: {}, amount_mode: 'signed', status_accepted: [] };
const PREVIEW_DEBOUNCE_MS = 300;

// Serwer zwraca czas w UTC — użytkownik ma widzieć swój.
const formatLocalDateTime = (iso: string) =>
  new Date(iso).toLocaleString('pl-PL', { dateStyle: 'short', timeStyle: 'short' });

const errorText = (err: unknown, fallback: string) => {
  if (err instanceof ApiError && err.data && typeof err.data === 'object' && 'detail' in err.data) {
    const detail = (err.data as { detail: unknown }).detail;
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail)) {
      return detail.map((d) => (typeof d === 'string' ? d : JSON.stringify(d))).join(' ');
    }
  }
  return fallback;
};

/**
 * Import wyciągu bankowego z CSV (docs/design/import-csv.md §8).
 *
 * Jedna strona, trzy sekcje: plik → mapowanie kolumn → podgląd z deduplikacją.
 * Gdy zapisany profil rozpozna plik i ma przypisane konto, mapowanie zwija się
 * do jednej linii i od razu widać podgląd — import to wtedy jedno kliknięcie.
 */
const BankImportPage = () => {
  const [step, setStep] = useState(1);
  const [searchParams] = useSearchParams();
  const { data: accountsData } = useApi(getAccounts, {
    errorMessage: 'Nie udało się wczytać kont.'
  });
  const accounts = useMemo(
    () => (accountsData?.accounts ?? []).filter((a) => !a.is_closed),
    [accountsData]
  );
  const batches = useApi(() => getBankImportBatches(), {
    errorMessage: 'Nie udało się wczytać historii importów.'
  });

  const [accountId, setAccountId] = useState(searchParams.get('account') ?? '');
  const [file, setFile] = useState<File | null>(null);
  const [inspected, setInspected] = useState<BankInspectResponse | null>(null);
  const [fileOptions, setFileOptions] = useState<BankParseOptions>({});
  const [mapping, setMapping] = useState<Mapping>(EMPTY_MAPPING);
  const [profileId, setProfileId] = useState<string | null>(null);
  const [mappingOpen, setMappingOpen] = useState(true);
  const [preview, setPreview] = useState<BankPreviewResponse | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [overrides, setOverrides] = useState<Record<number, RowOverride>>({});
  const [saveProfile, setSaveProfile] = useState(true);
  const [profileName, setProfileName] = useState('');
  const [committing, setCommitting] = useState(false);
  const [result, setResult] = useState<BankImportBatch | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const account = accounts.find((a) => a.id === accountId);
  const mappingErrors = inspected ? validateMapping(mapping) : [];
  // Podgląd z poprzedniego stanu (inne konto, niepełne mapowanie) nie może
  // zostać na ekranie — pokazujemy go tylko, gdy da się go policzyć teraz.
  const previewReady = Boolean(file && inspected && account && !mappingErrors.length);

  /**
   * `keepMapping`: `true` zostawia bieżące mapowanie; `'if-same-width'` zostawia
   * je tylko wtedy, gdy plik po zmianie ustawień ma tyle samo kolumn.
   */
  const runInspect = async (
    target: File,
    options: BankParseOptions,
    keepMapping: boolean | 'if-same-width'
  ) => {
    setError(null);
    try {
      const response = await inspectBankFile(target, options);
      if (keepMapping === 'if-same-width') {
        keepMapping = inspected?.columns.length === response.columns.length;
      }
      setInspected(response);
      setFileOptions(options);
      if (response.profile && !keepMapping) {
        setMapping({ ...EMPTY_MAPPING, ...response.profile.mapping });
        setProfileId(response.profile.id);
        setProfileName(response.profile.name);
        setSaveProfile(response.profile_match !== 'full');
        if (response.profile.account_id && !searchParams.get('account')) {
          setAccountId(response.profile.account_id);
        }
        setMappingOpen(response.profile_match !== 'full' || !response.profile.account_id);
      } else if (!keepMapping) {
        setMapping(suggestMapping(response.columns));
        setProfileId(null);
        setSaveProfile(true);
        setMappingOpen(true);
      }
    } catch (err) {
      setInspected(null);
      setError(errorText(err, 'Nie udało się odczytać pliku.'));
    }
  };

  const handleFile = (event: ChangeEvent<HTMLInputElement>) => {
    const picked = event.target.files?.[0] ?? null;
    setFile(picked);
    setPreview(null);
    setResult(null);
    setOverrides({});
    setInspected(null);
    if (picked) void runInspect(picked, {}, false);
  };

  // Podgląd liczy backend — po każdej zmianie mapowania, z małym opóźnieniem.
  useEffect(() => {
    if (!file || !inspected || !accountId || validateMapping(mapping).length) {
      return undefined;
    }
    let cancelled = false;
    const timer = window.setTimeout(async () => {
      setPreviewLoading(true);
      setPreviewError(null);
      try {
        const response = await previewBankFile(file, {
          account_id: accountId,
          options: fileOptions,
          mapping
        });
        if (!cancelled) {
          setPreview(response);
          setOverrides({});
        }
      } catch (err) {
        if (!cancelled) {
          setPreview(null);
          setPreviewError(errorText(err, 'Nie udało się przygotować podglądu.'));
        }
      } finally {
        if (!cancelled) setPreviewLoading(false);
      }
    }, PREVIEW_DEBOUNCE_MS);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [file, inspected, accountId, fileOptions, mapping]);

  const decisions = useMemo(
    () => (preview ? buildDecisions(preview.rows, overrides) : []),
    [preview, overrides]
  );
  const summary = preview ? summarize(preview.rows, decisions) : null;
  const missingTransferAccount = decisions.some(
    (d) => d.action === 'transfer' && !d.transfer_account_id
  );

  const handleOverride = (index: number, override: RowOverride | null) => {
    setOverrides((current) => {
      const next = { ...current };
      if (override) next[index] = override;
      else delete next[index];
      return next;
    });
  };

  const handleCommit = async () => {
    if (!file || !preview || !accountId) return;
    setCommitting(true);
    setError(null);
    try {
      const batch = await commitBankFile(file, {
        account_id: accountId,
        options: fileOptions,
        mapping,
        file_sha256: preview.file_sha256,
        decisions,
        profile_id: profileId,
        save_profile:
          saveProfile && profileName.trim()
            ? { name: profileName.trim(), bind_account: true }
            : null,
        file_name: file.name
      });
      setResult(batch);
      await batches.refetch();
      // Ponowny podgląd tego samego pliku pokazuje wszystko jako „już
      // zaimportowane" — widać od razu, że drugi import niczego nie zdubluje.
      await runInspect(file, fileOptions, true);
    } catch (err) {
      setError(errorText(err, 'Nie udało się zapisać importu.'));
    } finally {
      setCommitting(false);
    }
  };

  const handleUndo = async (batch: BankImportBatch) => {
    const ok = window.confirm(
      `Cofnąć import „${batch.file_name}” (${batch.account_name})?\n\n` +
        `Usunie ${batch.created_count} utworzonych transakcji i odepnie ` +
        `${batch.merged_count} scalonych.`
    );
    if (!ok) return;
    try {
      await undoBankImportBatch(batch.id);
      setResult(null);
      await batches.refetch();
      if (file && inspected) await runInspect(file, fileOptions, true);
    } catch (err) {
      setError(errorText(err, 'Nie udało się cofnąć importu.'));
    }
  };

  const mappedSummary = inspected?.profile && !mappingOpen;

  // Bez wczytanego pliku zawsze wracamy na pierwszy krok, np. po udanym imporcie
  const activeStep = inspected ? step : 1;

  return (
    <div className="page-wrapper compact-page">
      <div className="page-shell">
        <div className="page-header d-print-none">
          <h2 className="page-title">Import z banku</h2>
          <div className="text-secondary mt-1">
            Wgraj wyciąg CSV. Wiersze, które już są w aplikacji (z YNAB-a, z paragonów albo
            z wcześniejszego importu), zostaną rozpoznane i nie zdublują się.
          </div>
        </div>

        <div className="page-body">
          <ol className="wizard-steps" aria-label={copy.bankImport.stepsLabel}>
            {copy.bankImport.steps.map((label, index) => (
              <li
                key={label}
                className={`wizard-steps__step${activeStep === index + 1 ? ' is-active' : ''}${
                  activeStep > index + 1 ? ' is-done' : ''
                }`}
                aria-current={activeStep === index + 1 ? 'step' : undefined}
              >
                <span className="wizard-steps__number">{index + 1}</span>
                {label}
              </li>
            ))}
          </ol>

          {activeStep === 1 ? (
          <div className="card">
            <div className="card-body bank-import__file">
              <label className="form-label">
                Konto docelowe
                <select
                  className="form-select"
                  value={accountId}
                  onChange={(e) => setAccountId(e.target.value)}
                >
                  <option value="">— wybierz konto —</option>
                  {accounts.map((a) => (
                    <option key={a.id} value={a.id}>
                      {a.name} ({a.currency})
                    </option>
                  ))}
                </select>
              </label>
              <label className="form-label">
                Plik CSV
                <input
                  ref={fileInputRef}
                  type="file"
                  className="form-control"
                  accept=".csv,text/csv,text/plain"
                  onChange={handleFile}
                />
              </label>
            </div>
            {inspected?.previous_import ? (
              <div className="alert alert-info m-3 mt-0" role="status">
                Ten sam plik importowano już {formatLocalDateTime(inspected.previous_import.created_at)} na
                konto „{inspected.previous_import.account_name}”. Wiersze, które już są, zostaną
                pominięte.
              </div>
            ) : null}
            {error ? (
              <div className="alert alert-danger m-3 mt-0" role="alert">
                {error}
              </div>
            ) : null}
            <div className="card-footer wizard-steps__actions">
              <Button disabled={!inspected || !accountId} onClick={() => setStep(2)}>
                {copy.bankImport.next}
              </Button>
            </div>
          </div>
          ) : null}

          {inspected && activeStep === 2 ? (
            <div className="card mt-3">
              <div className="card-header">
                <h3 className="card-title">Mapowanie kolumn</h3>
                <div className="card-actions">
                  {mappedSummary ? (
                    <Button
                      variant="outline-secondary"
                      size="sm"
                      icon={<SettingsIcon size={16} />}
                      onClick={() => setMappingOpen(true)}
                    >
                      Zmień mapowanie
                    </Button>
                  ) : null}
                </div>
              </div>
              <div className="card-body">
                {mappedSummary ? (
                  <div>
                    Profil <strong>{inspected.profile?.name}</strong>
                    {account ? (
                      <>
                        {' '}· konto <strong>{account.name}</strong>
                      </>
                    ) : null}{' '}
                    · {inspected.row_count} wierszy w pliku
                  </div>
                ) : (
                  <>
                    {inspected.profile_match === 'partial' ? (
                      <div className="alert alert-warning" role="alert">
                        Plik przypomina profil „{inspected.profile?.name}”, ale część kolumn ma
                        inne nazwy niż przy zapisie. Sprawdź oznaczone kolumny.
                      </div>
                    ) : null}
                    <MappingSection
                      columns={inspected.columns}
                      mapping={mapping}
                      overrides={fileOptions}
                      detected={preview?.options ?? inspected.options}
                      mismatched={inspected.profile_mismatched_columns}
                      errors={mappingErrors}
                      onMappingChange={setMapping}
                      onFileOptionsChange={(options) => {
                        if (file) void runInspect(file, options, 'if-same-width');
                      }}
                      onValueOptionsChange={setFileOptions}
                    />
                  </>
                )}
              </div>
              <div className="card-footer wizard-steps__actions">
                <Button variant="outline-secondary" onClick={() => setStep(1)}>
                  {copy.bankImport.back}
                </Button>
                <Button disabled={!previewReady} onClick={() => setStep(3)}>
                  {copy.bankImport.next}
                </Button>
              </div>
            </div>
          ) : null}

          {previewError && activeStep > 1 ? (
            <div className="alert alert-danger mt-3" role="alert">
              {previewError}
            </div>
          ) : null}

          {activeStep === 3 && previewReady && preview && account ? (
            <div className="card mt-3">
              <div className="card-header">
                <h3 className="card-title">
                  Podgląd {previewLoading ? <span className="text-secondary">· odświeżam…</span> : null}
                </h3>
              </div>
              <div className="card-body">
                <PreviewSection
                  rows={preview.rows}
                  currency={account.currency}
                  accountId={account.id}
                  accounts={accounts}
                  overrides={overrides}
                  decisions={decisions}
                  onOverride={handleOverride}
                />
              </div>
              {summary ? (
                <div className="card-footer bank-import__footer">
                  <div>
                    Utworzysz <strong>{summary.create}</strong> · scalisz{' '}
                    <strong>{summary.merge}</strong> · przelewy <strong>{summary.transfer}</strong>{' '}
                    · pominiesz <strong>{summary.skip}</strong> · suma nowych{' '}
                    <strong>{formatMoney(summary.totalCents, account.currency)}</strong>
                  </div>
                  <label className="form-check mb-0">
                    <input
                      type="checkbox"
                      className="form-check-input"
                      checked={saveProfile}
                      onChange={(e) => setSaveProfile(e.target.checked)}
                    />
                    <span className="form-check-label">Zapisz mapowanie jako profil</span>
                  </label>
                  {saveProfile ? (
                    <input
                      className="form-control form-control-sm bank-import__profile-name"
                      placeholder="Nazwa profilu, np. ING"
                      aria-label="Nazwa profilu"
                      value={profileName}
                      onChange={(e) => setProfileName(e.target.value)}
                    />
                  ) : null}
                  <span className="flex-grow-1" />
                  <Button variant="outline-secondary" onClick={() => setStep(2)}>
                    {copy.bankImport.back}
                  </Button>
                  {missingTransferAccount ? (
                    <span className="text-warning small">Wskaż konto drugiej strony przelewu.</span>
                  ) : null}
                  <Button
                    icon={<FileSpreadsheetIcon size={16} />}
                    onClick={() => void handleCommit()}
                    disabled={
                      committing ||
                      previewLoading ||
                      !decisions.length ||
                      missingTransferAccount ||
                      (saveProfile && !profileName.trim())
                    }
                  >
                    {committing ? 'Importuję…' : 'Importuj'}
                  </Button>
                </div>
              ) : null}
            </div>
          ) : null}

          {result ? (
            <div className="alert alert-success mt-3" role="status">
              Zaimportowano: utworzono {result.created_count}, scalono {result.merged_count},
              pominięto {result.skipped_count}. <Link to="/transactions">Przejdź do transakcji</Link>
            </div>
          ) : null}

          <details className="card mt-3 bank-import__history">
            <summary className="card-header">
              <h3 className="card-title">Historia importów</h3>
            </summary>
            <div className="table-responsive">
              <table className="table table-sm table-vcenter card-table">
                <thead>
                  <tr>
                    <th>Kiedy</th>
                    <th>Plik</th>
                    <th>Konto</th>
                    <th>Okres</th>
                    <th className="text-end">Utworzone / scalone</th>
                    <th aria-label="Akcje" />
                  </tr>
                </thead>
                <tbody>
                  {(batches.data ?? []).map((batch) => (
                    <tr key={batch.id}>
                      <td className="text-nowrap">{formatLocalDateTime(batch.created_at)}</td>
                      <td>{batch.file_name}</td>
                      <td>{batch.account_name}</td>
                      <td className="text-nowrap">
                        {batch.date_from ? `${batch.date_from} – ${batch.date_to}` : '—'}
                      </td>
                      <td className="text-end">
                        {batch.created_count} / {batch.merged_count}
                      </td>
                      <td className="text-end">
                        <Button
                          variant="link"
                          size="sm"
                          icon={<Undo2Icon size={16} />}
                          onClick={() => void handleUndo(batch)}
                        >
                          Cofnij
                        </Button>
                      </td>
                    </tr>
                  ))}
                  {!batches.data?.length ? (
                    <tr>
                      <td colSpan={6} className="text-secondary text-center py-3">
                        Jeszcze nic nie zaimportowano.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </details>
        </div>
      </div>
    </div>
  );
};

export default BankImportPage;
