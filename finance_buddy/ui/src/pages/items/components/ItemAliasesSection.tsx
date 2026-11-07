// Nazwy z paragonów, które rozpoznajemy jako ten przedmiot. Każdy sklep drukuje
// ten sam produkt inaczej („ZUP.CHIN.VIFON”, „VIFON KURCZAK 70G”), a przedmiot
// ma jedną nazwę użytkownika. Błędnie przypięty alias da się tu odpiąć.
import { useEffect, useState } from 'react';
import { X } from 'lucide-react';
import { type ItemAlias, deleteItemAlias, getItemAliases } from '../../../api';

type ItemAliasesSectionProps = {
  itemId: string;
  disabled?: boolean;
};

const ItemAliasesSection = ({ itemId, disabled = false }: ItemAliasesSectionProps) => {
  const [aliases, setAliases] = useState<ItemAlias[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [removingId, setRemovingId] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    getItemAliases(itemId)
      .then((data) => {
        if (!cancelled) setAliases(data);
      })
      .catch((err: unknown) => {
        console.error(err);
        if (!cancelled) setError('Nie udało się wczytać nazw z paragonów.');
      });
    return () => {
      cancelled = true;
    };
  }, [itemId]);

  const handleRemove = async (alias: ItemAlias) => {
    try {
      setRemovingId(alias.id);
      setError(null);
      await deleteItemAlias(itemId, alias.id);
      setAliases((prev) => (prev ?? []).filter((entry) => entry.id !== alias.id));
    } catch (err) {
      console.error(err);
      setError('Nie udało się odpiąć nazwy.');
    } finally {
      setRemovingId(null);
    }
  };

  return (
    <section className="item-aliases" aria-labelledby="item-aliases-title">
      <h4 className="item-section__title" id="item-aliases-title">
        Nazwy z paragonów
      </h4>
      <p className="form-hint mb-2">
        Paragon z taką nazwą trafi w ten przedmiot. Nazwy dopisują się same, gdy zatwierdzasz paragon.
      </p>
      {error && <div className="alert alert-danger py-2">{error}</div>}
      {aliases === null && !error && <p className="text-secondary small mb-0">Wczytywanie…</p>}
      {aliases?.length === 0 && (
        <p className="text-secondary small mb-0">Żaden paragon jeszcze nie wskazał tego przedmiotu.</p>
      )}
      {aliases && aliases.length > 0 && (
        <ul className="item-aliases__list">
          {aliases.map((alias) => (
            <li key={alias.id} className="item-aliases__row">
              <span className="item-aliases__name">{alias.alias}</span>
              <span className="item-aliases__store">{alias.store_name ?? 'Wszystkie sklepy'}</span>
              <button
                type="button"
                className="btn btn-ghost-secondary btn-icon btn-sm is-focusable"
                aria-label={`Odepnij nazwę ${alias.alias}`}
                title="Odepnij"
                onClick={() => void handleRemove(alias)}
                disabled={disabled || removingId !== null}
              >
                <X size={14} aria-hidden="true" />
              </button>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
};

export default ItemAliasesSection;
