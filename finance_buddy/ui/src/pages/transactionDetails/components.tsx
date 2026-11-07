// Klocki widoku szczegółów transakcji: etykieta kategorii, zdjęcia, historia ze sklepem.
import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { CircleAlertIcon, FileIcon, PlusIcon, Trash2Icon } from 'lucide-react';
import { getTransactionsPage, type Category, type Transaction, type TransactionAttachment } from '../../api';
import Money from '../../components/ui/Money';
import { useAuthorizedObjectUrl } from '../../hooks/useAuthorizedObjectUrl';
import { formatShortDate } from '../../utils/date';

/** „Jedzenie › ” wyszarzone, nazwa kategorii normalnie; `leafOnly` zostawia samą nazwę (np. w wąskiej kolumnie). */
export const CategoryLabel = ({
  path,
  empty = 'Bez kategorii',
  leafOnly = false
}: {
  path: string;
  empty?: string;
  leafOnly?: boolean;
}) => {
  const segments = path.split('/').map((segment) => segment.trim()).filter(Boolean);
  if (!segments.length) {
    return (
      <span className="tx-detail__category">
        <span className="tx-detail__muted">{empty}</span>
      </span>
    );
  }
  const name = segments.pop();
  return (
    <span className="tx-detail__category" title={[...segments, name].join(' › ')}>
      {!leafOnly && segments.length > 0 && <span className="tx-detail__muted">{segments.join(' › ')} › </span>}
      {name}
    </span>
  );
};

export const CategoryDot = ({ color }: { color: string }) => (
  <span className="tx-detail__dot" style={{ background: color }} aria-hidden="true" />
);

const formatFileSize = (bytes: number) => {
  if (!bytes || bytes < 0) return '';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${Math.round(bytes / 1024)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1).replace('.', ',')} MB`;
};

type PhotoTileProps = {
  attachment: TransactionAttachment;
  deleting: boolean;
  onDelete: (attachmentId: string) => void;
};

/**
 * Miniatura załącznika. Plik pobiera przez `useAuthorizedObjectUrl`, bo endpoint
 * załączników wymaga tokenu — surowy adres API w `src`/`href` zwróciłby 401.
 */
const PhotoTile = ({ attachment, deleting, onDelete }: PhotoTileProps) => {
  const isImage = (attachment.content_type ?? '').startsWith('image/');
  const { url, loading, error } = useAuthorizedObjectUrl(attachment.download_url, {
    errorMessage: 'Nie udało się wczytać załącznika.'
  });
  const created = attachment.created_at ? formatShortDate(attachment.created_at.slice(0, 10)) : '';
  const meta = [created, formatFileSize(attachment.size_bytes)].filter(Boolean).join(' · ');

  const preview = isImage && url ? (
    <img src={url} alt="" />
  ) : loading ? (
    <span className="spinner-border spinner-border-sm" role="status" aria-label="Wczytywanie załącznika" />
  ) : error ? (
    <CircleAlertIcon size={20} aria-hidden="true" />
  ) : (
    <FileIcon size={20} aria-hidden="true" />
  );

  return (
    <div className="tx-photo">
      {url ? (
        <a className="tx-photo__preview" href={url} target="_blank" rel="noreferrer" aria-label={`Otwórz ${attachment.original_name}`}>
          {preview}
        </a>
      ) : (
        <div className="tx-photo__preview" title={error ?? undefined}>
          {preview}
        </div>
      )}
      <button
        type="button"
        className="icon-button tx-photo__delete"
        aria-label={`Usuń ${attachment.original_name}`}
        title="Usuń"
        disabled={deleting}
        onClick={() => onDelete(attachment.id)}
      >
        <Trash2Icon size={14} aria-hidden="true" />
      </button>
      <span className="tx-photo__name" title={attachment.original_name}>
        {attachment.original_name}
      </span>
      {meta && <span className="tx-detail__muted tx-photo__meta">{meta}</span>}
    </div>
  );
};

type PhotosSectionProps = {
  attachments: TransactionAttachment[];
  uploading: boolean;
  removingId: string | null;
  error: string | null;
  onAdd: () => void;
  onDelete: (attachmentId: string) => void;
};

export const PhotosSection = ({ attachments, uploading, removingId, error, onAdd, onDelete }: PhotosSectionProps) => (
  <section className="tx-card tx-photos" aria-label="Zdjęcia paragonu">
    <h2 className="tx-card__title">
      Zdjęcia <span className="tx-detail__muted tx-card__count">{attachments.length}</span>
    </h2>
    {error && (
      <p className="transactions-error" role="alert">
        {error}
      </p>
    )}
    <div className="tx-photos__grid">
      {attachments.map((attachment) => (
        <PhotoTile key={attachment.id} attachment={attachment} deleting={removingId === attachment.id} onDelete={onDelete} />
      ))}
      <button type="button" className="tx-photos__add" onClick={onAdd} disabled={uploading}>
        <PlusIcon size={18} aria-hidden="true" />
        {uploading ? 'Ładowanie…' : 'Dodaj zdjęcie'}
      </button>
    </div>
  </section>
);

const HISTORY_LIMIT = 5;

type StoreHistoryProps = {
  transactionId: string;
  storeId: string;
  storeLabel: string;
  categoriesById: ReadonlyMap<string, Category>;
};

/** Poprzednie transakcje z tym samym sklepem — przy wydatkach bez pozycji to najlepszy kontekst. */
export const StoreHistory = ({ transactionId, storeId, storeLabel, categoriesById }: StoreHistoryProps) => {
  // Wynik pamięta, dla której transakcji go pobrano — inna transakcja to znów „wczytywanie”
  const key = `${storeId}:${transactionId}`;
  const [loaded, setLoaded] = useState<{ key: string; rows: Transaction[] } | null>(null);
  const rows = loaded?.key === key ? loaded.rows : null;

  useEffect(() => {
    let cancelled = false;
    getTransactionsPage({ store_ids: [storeId], limit: HISTORY_LIMIT + 1 })
      .then((page) => {
        if (cancelled) return;
        const previous = page.items.filter((row) => row.id !== transactionId).slice(0, HISTORY_LIMIT);
        setLoaded({ key: `${storeId}:${transactionId}`, rows: previous });
      })
      .catch((err) => {
        console.error(err);
        if (!cancelled) setLoaded({ key: `${storeId}:${transactionId}`, rows: [] });
      });
    return () => {
      cancelled = true;
    };
  }, [storeId, transactionId]);

  if (rows !== null && rows.length === 0) return null;

  return (
    <section className="tx-card tx-history" aria-label="Historia z tym sklepem">
      <header className="tx-card__header">
        <h2 className="tx-card__title">Wcześniej: {storeLabel}</h2>
      </header>
      {rows === null ? (
        <p className="tx-detail__muted tx-history__loading">Wczytywanie…</p>
      ) : (
        rows.map((row) => {
          const path = row.category_id ? categoriesById.get(row.category_id)?.path ?? '' : '';
          return (
            <Link key={row.id} to={`/transactions/${row.id}`} className="tx-history__row">
              <span className="tx-detail__muted">{row.date ? formatShortDate(row.date) : '—'}</span>
              <span className="tx-history__category">
                <CategoryLabel path={path} empty={row.items?.length ? 'Z pozycjami' : 'Bez kategorii'} />
              </span>
              <Money cents={row.total_cents} currency={row.currency} />
            </Link>
          );
        })
      )}
      <Link to={`/transactions?stores=${encodeURIComponent(storeId)}`} className="tx-history__more">
        Wszystkie transakcje z tym sklepem →
      </Link>
    </section>
  );
};
