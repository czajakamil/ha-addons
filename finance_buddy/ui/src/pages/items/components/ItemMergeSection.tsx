// Scalenie bieżącego przedmiotu z innym: pozycje transakcji i nazwy z paragonów
// przechodzą do wybranego przedmiotu, a ten znika. Przed scaleniem pokazujemy
// skutki z podglądu (dry run), bo operacji nie da się cofnąć jednym kliknięciem.
import { useMemo, useState } from 'react';
import { type Item, type ItemMergeResult, mergeItems } from '../../../api';
import EntityAutocomplete, {
  type EntityOption
} from '../../../components/transactions/EntityAutocomplete';
import { describeMerge, getItemLabel, pozycje } from '../model';

type ItemMergeSectionProps = {
  item: Item;
  items: Item[];
  /** Liczba pozycji transakcji per przedmiot — podpowiedź przy wyborze celu. */
  usageById: Map<string, number>;
  disabled?: boolean;
  onBusyChange: (busy: boolean) => void;
  onMerged: (sourceId: string, targetId: string) => void;
};

const ItemMergeSection = ({
  item,
  items,
  usageById,
  disabled = false,
  onBusyChange,
  onMerged
}: ItemMergeSectionProps) => {
  const [targetId, setTargetId] = useState('');
  const [preview, setPreview] = useState<ItemMergeResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const options = useMemo<EntityOption[]>(
    () =>
      items
        .filter((entry) => entry.id !== item.id)
        .map((entry) => {
          const used = usageById.get(entry.id) ?? 0;
          return { id: entry.id, label: entry.name, hint: used ? pozycje(used) : undefined };
        }),
    [items, item.id, usageById]
  );
  const target = items.find((entry) => entry.id === targetId) ?? null;

  const run = async (dryRun: boolean) => {
    if (!targetId) return;
    try {
      setLoading(true);
      onBusyChange(true);
      setError(null);
      const result = await mergeItems([item.id], targetId, dryRun);
      if (dryRun) {
        setPreview(result);
      } else {
        onMerged(item.id, targetId);
      }
    } catch (err) {
      console.error(err);
      setError(dryRun ? 'Nie udało się policzyć skutków scalenia.' : 'Nie udało się scalić przedmiotów.');
    } finally {
      setLoading(false);
      onBusyChange(false);
    }
  };

  const busy = disabled || loading;

  return (
    <section className="item-merge" aria-labelledby="item-merge-title">
      <h4 className="item-section__title" id="item-merge-title">
        Scal z innym przedmiotem
      </h4>
      <p className="form-hint mb-2">
        Ten sam produkt pod inną nazwą? Wskaż właściwy przedmiot, a pozycje i nazwy z paragonów przejdą do
        niego.
      </p>
      {error && <div className="alert alert-danger py-2">{error}</div>}
      <div className="item-merge__row">
        <div className="item-merge__picker">
          <EntityAutocomplete
            inputId="item-merge-target"
            value={targetId}
            options={options}
            onSelect={(id) => {
              setTargetId(id);
              setPreview(null);
              setError(null);
            }}
            emptyOptionLabel="— wybierz przedmiot —"
            placeholder="Wpisz, aby wyszukać przedmiot"
            disabled={busy}
          />
        </div>
        {!preview && (
          <button
            type="button"
            className="btn btn-outline-secondary is-focusable"
            onClick={() => void run(true)}
            disabled={busy || !targetId}
          >
            {loading ? 'Liczenie…' : 'Scal…'}
          </button>
        )}
      </div>
      {preview && target && (
        <div className="item-merge__confirm" role="status">
          <p className="mb-2">{describeMerge(preview, getItemLabel(item), target.name)}</p>
          <div className="d-flex gap-2">
            <button
              type="button"
              className="btn btn-danger btn-sm is-focusable"
              onClick={() => void run(false)}
              disabled={busy}
            >
              {loading ? 'Scalanie…' : 'Tak, scal'}
            </button>
            <button
              type="button"
              className="btn btn-link link-secondary btn-sm is-focusable"
              onClick={() => setPreview(null)}
              disabled={busy}
            >
              Nie
            </button>
          </div>
        </div>
      )}
    </section>
  );
};

export default ItemMergeSection;
