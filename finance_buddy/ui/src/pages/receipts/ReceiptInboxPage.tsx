import { useCallback, useEffect, useRef, useState, type ChangeEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import { FileInputIcon, ReceiptIcon, RefreshCwIcon, Trash2Icon } from 'lucide-react';
import Button from '../../components/Button';
import {
  deleteReceiptDraft,
  getReceiptDrafts,
  uploadReceiptsWithProgress,
  type ReceiptDraftCounts,
  type ReceiptDraftSummary,
  type ReceiptQueueFilter
} from '../../api';
import EmptyState from '../../components/ui/EmptyState';
import PageHeader from '../../components/ui/PageHeader';
import SegmentedControl from '../../components/ui/SegmentedControl';
import SkeletonRows from '../../components/ui/SkeletonRows';
import TruncatedText from '../../components/ui/TruncatedText';
import { formatShortDate } from '../../utils/date';
import { formatMoney } from '../../utils/format';
import ResizableTable from '../../components/ui/ResizableTable';

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

  // Wybór plików od razu startuje rozpoznawanie: nic nie trafia jeszcze do transakcji,
  // a szkic zawsze można usunąć.
  const handleFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const selectedFiles = Array.from(event.target.files ?? []);
    event.target.value = '';
    if (!selectedFiles.length) return;

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

  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const openFirst = () => {
    const first = drafts[0];
    if (first) navigate(`/receipts/${first.id}?filter=${filter}`);
  };

  return (
    <div className="page-shell page-stack-layout receipt-inbox">
      <PageHeader
        title="Paragony"
        count={counts.todo}
        actions={
          <>
            <button
              type="button"
              className="text-button"
              onClick={() => void load(filter)}
              disabled={loading}
            >
              <RefreshCwIcon size={14} aria-hidden="true" />
              Odśwież
            </button>
            <Button
              icon={<FileInputIcon size={16} aria-hidden="true" />}
              onClick={() => fileInputRef.current?.click()}
              disabled={uploading}
            >
              {uploading ? 'Przetwarzanie…' : 'Wgraj paragony'}
            </Button>
            <input
              ref={fileInputRef}
              type="file"
              className="d-none"
              accept="application/pdf,image/*"
              multiple
              aria-label="Pliki paragonów"
              onChange={(event) => void handleFileChange(event)}
            />
          </>
        }
      />

      {progress !== null && (
        <div className="receipt-inbox__progress" role="status">
          <div className="receipt-inbox__progress-label">
            <span>{progressMessage}</span>
            <span>{Math.round(progress)}%</span>
          </div>
          <div className="progress-bar">
            <span style={{ width: `${progress}%`, transition: 'width 220ms ease' }} />
          </div>
        </div>
      )}

      {error && (
        <p className="transactions-error" role="alert">
          {error}
        </p>
      )}

      <div className="receipt-inbox__toolbar">
        <SegmentedControl
          label="Stan paragonów"
          value={filter}
          options={[
            { value: 'todo', label: 'Do zrobienia', count: counts.todo },
            { value: 'done', label: 'Zatwierdzone', count: counts.committed },
            { value: 'skipped', label: 'Pominięte', count: counts.skipped },
            { value: 'all', label: 'Wszystkie', count: counts.total }
          ]}
          onChange={setFilter}
        />
        {drafts.length > 0 && (
          <button type="button" className="text-button" onClick={openFirst}>
            Przeglądaj po kolei
          </button>
        )}
      </div>

      <section className="receipt-inbox__card">
        {loading && !drafts.length && <SkeletonRows rows={6} />}
        {!loading && !drafts.length && (
          <EmptyState
            icon={ReceiptIcon}
            message="Nic tu nie ma. Wgrane paragony czekają tu jako szkice, a transakcja powstaje dopiero po zatwierdzeniu."
          />
        )}
        {drafts.length > 0 && (
          <div className="table-responsive">
            <ResizableTable tableId="receipts" className="receipt-inbox__table">
              <thead>
                <tr>
                  <th>Sklep</th>
                  <th className="receipt-inbox__date">Data</th>
                  <th className="receipt-inbox__amount">Suma</th>
                  <th className="receipt-inbox__status">Stan</th>
                  <th className="receipt-inbox__actions" aria-label="Akcje" />
                </tr>
              </thead>
              <tbody>
                {drafts.map((draft) => {
                  const worst = draft.issues.find((issue) => issue.level === 'bad') ?? draft.issues[0];
                  return (
                    <tr
                      key={draft.id}
                      className="receipt-inbox__row"
                      onClick={() => navigate(`/receipts/${draft.id}?filter=${filter}`)}
                    >
                      <td>
                        <TruncatedText text={draft.store_name || draft.source_name} />
                        <span className="receipt-inbox__meta">
                          <TruncatedText
                            text={`${draft.source_name}, s. ${draft.page_number} · pozycje: ${draft.item_count}`}
                          />
                        </span>
                      </td>
                      <td className="receipt-inbox__date">
                        {draft.happened_at ? formatShortDate(draft.happened_at.slice(0, 10)) : '—'}
                      </td>
                      <td className="receipt-inbox__amount">
                        {draft.total_cents == null ? '—' : formatMoney(draft.total_cents, draft.currency)}
                      </td>
                      <td className="receipt-inbox__status">
                        {draft.status === 'committed' ? (
                          <span className="receipt-badge receipt-badge--ok">zatwierdzony</span>
                        ) : draft.status === 'skipped' ? (
                          <span className="receipt-badge">pominięty</span>
                        ) : worst ? (
                          <span
                            className={`receipt-badge receipt-badge--${worst.level === 'bad' ? 'bad' : 'warn'}`}
                            title={worst.message}
                          >
                            {worst.message}
                          </span>
                        ) : (
                          <span className="receipt-badge receipt-badge--ok">gotowy</span>
                        )}
                      </td>
                      <td className="receipt-inbox__actions">
                        <button
                          type="button"
                          className="icon-button"
                          title="Usuń szkic"
                          aria-label="Usuń szkic"
                          onClick={(event) => {
                            event.stopPropagation();
                            void handleDelete(draft);
                          }}
                        >
                          <Trash2Icon size={14} aria-hidden="true" />
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </ResizableTable>
          </div>
        )}
      </section>
    </div>
  );
};

export default ReceiptInboxPage;
