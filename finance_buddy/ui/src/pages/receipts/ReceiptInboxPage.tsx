import { useCallback, useEffect, useRef, useState, type ChangeEvent, type FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import { FileInputIcon, ImageIcon, RefreshCwIcon, Trash2Icon, TriangleAlertIcon } from 'lucide-react';
import Button from '../../components/Button';
import {
  deleteReceiptDraft,
  getReceiptDrafts,
  uploadReceiptsWithProgress,
  type ReceiptDraftCounts,
  type ReceiptDraftSummary,
  type ReceiptQueueFilter
} from '../../api';
import { formatMoney } from '../../utils/format';

const FILTERS: { value: ReceiptQueueFilter; label: string }[] = [
  { value: 'todo', label: 'Do zrobienia' },
  { value: 'done', label: 'Zatwierdzone' },
  { value: 'skipped', label: 'Pominięte' },
  { value: 'all', label: 'Wszystkie' }
];

const EMPTY_COUNTS: ReceiptDraftCounts = {
  total: 0,
  todo: 0,
  review: 0,
  skipped: 0,
  committed: 0
};

/**
 * Skrzynka paragonów — wsad po OCR, zanim stanie się transakcjami.
 *
 * Wgranie plików nie zapisuje niczego w transakcjach: powstają szkice, które
 * można przeglądać na raty. Ta lista jest tylko nawigacją — właściwa praca dzieje
 * się w pełnoekranowym edytorze, gdzie obok formularza stoi obraz paragonu.
 */
const ReceiptInboxPage = () => {
  const navigate = useNavigate();
  const [filter, setFilter] = useState<ReceiptQueueFilter>('todo');
  const [drafts, setDrafts] = useState<ReceiptDraftSummary[]>([]);
  const [counts, setCounts] = useState<ReceiptDraftCounts>(EMPTY_COUNTS);
  const [loading, setLoading] = useState(false);

  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState<number | null>(null);
  const [progressMessage, setProgressMessage] = useState('');
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const load = useCallback(
    async (mode: ReceiptQueueFilter) => {
      setLoading(true);
      try {
        const response = await getReceiptDrafts(mode);
        setDrafts(response.drafts);
        setCounts(response.counts);
      } catch (err) {
        console.error(err);
        setError('Nie udało się wczytać skrzynki.');
      } finally {
        setLoading(false);
      }
    },
    []
  );

  useEffect(() => {
    void load(filter);
  }, [filter, load]);

  useEffect(
    () => () => {
      abortRef.current?.abort();
    },
    []
  );

  const handleFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    setSelectedFiles(Array.from(event.target.files ?? []));
    setError(null);
  };

  const handleUpload = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!selectedFiles.length) {
      setError('Wybierz co najmniej jeden plik PDF albo zdjęcie.');
      return;
    }

    const controller = new AbortController();
    abortRef.current = controller;
    setUploading(true);
    setError(null);
    setProgress(5);
    setProgressMessage('Ładowanie plików…');

    try {
      const response = await uploadReceiptsWithProgress(selectedFiles, {
        onUploadProgress: (loaded, total) => {
          setProgress(Math.min(50, Math.max(8, (loaded / (total || 1)) * 45 + 5)));
        },
        onStateChange: (state) => {
          if (state === 'processing') {
            setProgress((current) => Math.max(current ?? 50, 60));
            setProgressMessage('OCR w toku — to potrafi chwilę potrwać…');
          }
        },
        signal: controller.signal
      });
      setCounts(response.counts);
      setSelectedFiles([]);
      setProgress(100);
      setProgressMessage(`Wgrano ${response.counts.total} stron do skrzynki.`);
      await load(filter);
    } catch (err) {
      if (err instanceof DOMException && err.name === 'AbortError') return;
      console.error(err);
      setError(err instanceof Error ? err.message : 'Nie udało się przetworzyć plików.');
    } finally {
      abortRef.current = null;
      setUploading(false);
      window.setTimeout(() => {
        setProgress(null);
        setProgressMessage('');
      }, 900);
    }
  };

  const handleDelete = async (draft: ReceiptDraftSummary) => {
    try {
      await deleteReceiptDraft(draft.id);
      await load(filter);
    } catch (err) {
      console.error(err);
      setError('Nie udało się usunąć szkicu.');
    }
  };

  const openFirst = () => {
    const first = drafts[0];
    if (first) navigate(`/receipts/${first.id}?filter=${filter}`);
  };

  return (
    <div className="page-wrapper compact-page">
      <div className="page-shell">
        <div className="page-header d-print-none">
          <div className="row align-items-center">
            <div className="col">
              <h2 className="page-title">Paragony</h2>
              <div className="text-secondary mt-1">
                Wgrane paragony czekają tu jako szkice — transakcja powstaje dopiero po
                zatwierdzeniu w edytorze.
              </div>
            </div>
          </div>
        </div>

        <div className="page-body">
          <div className="card">
            <div className="card-body">
              <form className="d-flex flex-wrap align-items-center gap-2" onSubmit={handleUpload}>
                <input
                  type="file"
                  className="form-control receipt-inbox__file"
                  accept="application/pdf,image/*"
                  multiple
                  onChange={handleFileChange}
                  disabled={uploading}
                />
                <Button
                  type="submit"
                  icon={<FileInputIcon size={16} />}
                  disabled={uploading || !selectedFiles.length}
                >
                  {uploading ? 'Przetwarzanie…' : 'Wgraj i rozpoznaj'}
                </Button>
                <Button
                  variant="outline-secondary"
                  icon={<RefreshCwIcon size={16} />}
                  onClick={() => void load(filter)}
                  disabled={loading}
                >
                  Odśwież
                </Button>
              </form>

              {progress !== null ? (
                <div className="mt-3">
                  <div className="d-flex justify-content-between text-secondary small mb-1">
                    <span>{progressMessage}</span>
                    <span>{Math.round(progress)}%</span>
                  </div>
                  <div className="progress-bar">
                    <span style={{ width: `${progress}%`, transition: 'width 220ms ease' }} />
                  </div>
                </div>
              ) : null}

              {error ? (
                <div className="alert alert-danger mt-3 mb-0" role="alert">
                  {error}
                </div>
              ) : null}
            </div>
          </div>

          <div className="receipt-inbox__toolbar mt-3">
            <div className="btn-group" role="group">
              {FILTERS.map((option) => (
                <button
                  key={option.value}
                  type="button"
                  className={`btn btn-sm ${
                    filter === option.value ? 'btn-primary' : 'btn-outline-secondary'
                  }`}
                  onClick={() => setFilter(option.value)}
                >
                  {option.label}
                </button>
              ))}
            </div>
            <span className="flex-grow-1" />
            {counts.review ? (
              <span className="receipt-badge receipt-badge--warn">
                <TriangleAlertIcon size={14} /> do przejrzenia: {counts.review}
              </span>
            ) : null}
            <span className="text-secondary small">
              do zrobienia {counts.todo} · zatwierdzone {counts.committed} · pominięte{' '}
              {counts.skipped}
            </span>
            <Button size="sm" onClick={openFirst} disabled={!drafts.length}>
              Otwórz pierwszy
            </Button>
          </div>

          <div className="card mt-2">
            <div className="table-responsive">
              <table className="table table-vcenter card-table">
                <thead>
                  <tr>
                    <th>Plik</th>
                    <th>Sklep</th>
                    <th>Data</th>
                    <th className="text-end">Suma</th>
                    <th className="text-end">Pozycje</th>
                    <th>Stan</th>
                    <th aria-label="Akcje" />
                  </tr>
                </thead>
                <tbody>
                  {drafts.map((draft) => {
                    const worst = draft.issues.find((issue) => issue.level === 'bad')
                      ?? draft.issues[0];
                    return (
                      <tr
                        key={draft.id}
                        className="receipt-inbox__row"
                        onClick={() => navigate(`/receipts/${draft.id}?filter=${filter}`)}
                      >
                        <td>
                          <div className="d-flex align-items-center gap-2">
                            <ImageIcon size={16} className="text-secondary" />
                            <span>
                              {draft.source_name}
                              <span className="text-secondary"> · s.{draft.page_number}</span>
                            </span>
                          </div>
                        </td>
                        <td>{draft.store_name || <span className="text-secondary">—</span>}</td>
                        <td>
                          {draft.happened_at ? (
                            draft.happened_at.slice(0, 16).replace('T', ' ')
                          ) : (
                            <span className="text-secondary">—</span>
                          )}
                        </td>
                        <td className="text-end">
                          {draft.total_cents == null ? (
                            <span className="text-secondary">—</span>
                          ) : (
                            formatMoney(draft.total_cents, draft.currency)
                          )}
                        </td>
                        <td className="text-end">{draft.item_count}</td>
                        <td>
                          {draft.status === 'committed' ? (
                            <span className="receipt-badge receipt-badge--ok">zatwierdzony</span>
                          ) : draft.status === 'skipped' ? (
                            <span className="receipt-badge">pominięty</span>
                          ) : worst ? (
                            <span
                              className={`receipt-badge receipt-badge--${
                                worst.level === 'bad' ? 'bad' : 'warn'
                              }`}
                              title={worst.message}
                            >
                              {worst.message}
                            </span>
                          ) : (
                            <span className="receipt-badge receipt-badge--ok">gotowy</span>
                          )}
                        </td>
                        <td className="text-end">
                          <Button
                            variant="link"
                            size="sm"
                            icon={<Trash2Icon size={16} />}
                            onClick={(event) => {
                              event.stopPropagation();
                              void handleDelete(draft);
                            }}
                            aria-label="Usuń szkic"
                          >
                            <span className="visually-hidden">Usuń szkic</span>
                          </Button>
                        </td>
                      </tr>
                    );
                  })}
                  {!drafts.length && !loading ? (
                    <tr>
                      <td colSpan={7} className="text-secondary text-center py-4">
                        Nic tu nie ma. Wgraj paragony powyżej — PDF-y wielostronicowe rozbijamy na
                        pojedyncze paragony.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ReceiptInboxPage;
