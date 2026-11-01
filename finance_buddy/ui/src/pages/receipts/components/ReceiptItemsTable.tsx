import {
  useMemo,
  useRef,
  type KeyboardEvent as ReactKeyboardEvent,
  type MouseEvent as ReactMouseEvent
} from 'react';
import { IconGripVertical, IconPlus, IconX } from '@tabler/icons-react';
import Button from '../../../components/Button';
import TagPathInput from '../../../components/tags/TagPathInput';
import EntityAutocomplete, {
  type EntityOption
} from '../../../components/transactions/EntityAutocomplete';
import type { Category, Item } from '../../../api';
import {
  NEW_PRODUCT,
  NO_PRODUCT,
  centsToInput,
  inputToCents,
  inputToNumber,
  rowsBalance,
  type ItemRow
} from '../model';

type ReceiptItemsTableProps = {
  rows: ItemRow[];
  products: Item[];
  categories: Category[];
  total: string;
  receiptDiscount: string;
  matching: boolean;
  onChange: (index: number, patch: Partial<ItemRow>) => void;
  onMove: (index: number, target: number) => void;
  onRemove: (index: number) => void;
  onAdd: () => void;
};

const money = (cents: number) => (cents / 100).toFixed(2);

/**
 * Pozycje paragonu na pełnej szerokości: przepisywanie, kolejność i produkt.
 *
 * Kolejność wierszy jest zapisywana, więc warto trzymać ją zgodną z wydrukiem —
 * stąd uchwyt do przeciągania i Alt+↑/↓ dla ręki, która i tak jest na klawiaturze.
 */
const ReceiptItemsTable = ({
  rows,
  products,
  categories,
  total,
  receiptDiscount,
  matching,
  onChange,
  onMove,
  onRemove,
  onAdd
}: ReceiptItemsTableProps) => {
  const dragIndex = useRef<number | null>(null);
  const { gross, discounts, balance } = rowsBalance(rows, receiptDiscount);
  const totalCents = inputToCents(total);
  const productOptions = useMemo<EntityOption[]>(
    () => products.map((product) => ({ id: product.id, label: product.name })),
    [products]
  );

  /** Wartość liczy się z ilość × cena, dopóki nie zostanie wpisana ręcznie. */
  const recompute = (index: number, patch: Partial<ItemRow>) => {
    const row = { ...rows[index], ...patch };
    if (row.lineTotalManual) {
      onChange(index, patch);
      return;
    }
    const quantity = inputToNumber(row.quantity);
    const price = inputToCents(row.unitPrice);
    const lineTotal =
      quantity != null && price != null ? centsToInput(Math.round(quantity * price)) : row.lineTotal;
    onChange(index, { ...patch, lineTotal });
  };

  // Przeciąganie na zdarzeniach myszy: natywne HTML5 drag-and-drop gubi się nad
  // polami input wewnątrz wiersza.
  const startDrag = (index: number, event: ReactMouseEvent) => {
    event.preventDefault();
    dragIndex.current = index;

    const onMouseMove = (move: MouseEvent) => {
      const from = dragIndex.current;
      if (from == null) return;
      const rowsInDom = Array.from(document.querySelectorAll('[data-receipt-row]'));
      const target = rowsInDom.findIndex((element) => {
        const box = element.getBoundingClientRect();
        return move.clientY < box.top + box.height / 2;
      });
      const to = target === -1 ? rowsInDom.length - 1 : Math.max(0, target - (target > from ? 1 : 0));
      if (to !== from) {
        onMove(from, to);
        dragIndex.current = to;
      }
    };
    const onMouseUp = () => {
      dragIndex.current = null;
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  };

  const handleRowKeyDown = (index: number, event: ReactKeyboardEvent) => {
    if (!event.altKey || (event.key !== 'ArrowUp' && event.key !== 'ArrowDown')) return;
    const target = index + (event.key === 'ArrowUp' ? -1 : 1);
    if (target < 0 || target >= rows.length) return;
    event.preventDefault();
    onMove(index, target);
  };

  return (
    <div className="receipt-items">
      <div className="table-responsive">
        <table className="table table-sm table-vcenter receipt-items__table mb-0">
          <thead>
            <tr>
              <th className="receipt-items__col-drag" aria-label="Kolejność" />
              <th>Nazwa z paragonu</th>
              <th className="receipt-items__col-code">Kod</th>
              <th className="receipt-items__col-num">Ilość</th>
              <th className="receipt-items__col-unit">Jm</th>
              <th className="receipt-items__col-num">Cena</th>
              <th className="receipt-items__col-num">Wartość</th>
              <th className="receipt-items__col-num">Rabat</th>
              <th className="receipt-items__col-product">Produkt</th>
              <th className="receipt-items__col-drag" aria-label="Usuń" />
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => {
              const discountCents = inputToCents(row.discount);
              const isNew = row.productId === NEW_PRODUCT;
              return (
                <tr
                  key={row.key}
                  data-receipt-row
                  onKeyDown={(event) => handleRowKeyDown(index, event)}
                  className={row.productId === NO_PRODUCT ? 'receipt-items__row--skipped' : undefined}
                >
                  <td
                    className="receipt-items__drag"
                    title="Przeciągnij, aby zmienić kolejność (albo Alt+↑/↓)"
                    onMouseDown={(event) => startDrag(index, event)}
                  >
                    <IconGripVertical size={16} />
                  </td>
                  <td>
                    <input
                      className="form-control form-control-sm"
                      value={row.rawName}
                      onChange={(event) => onChange(index, { rawName: event.target.value })}
                    />
                    {discountCents ? (
                      <input
                        className="form-control form-control-sm mt-1"
                        placeholder="opis rabatu z paragonu"
                        value={row.discountLabel}
                        onChange={(event) =>
                          onChange(index, { discountLabel: event.target.value })
                        }
                      />
                    ) : null}
                  </td>
                  <td>
                    <input
                      className="form-control form-control-sm"
                      value={row.productCode}
                      onChange={(event) => onChange(index, { productCode: event.target.value })}
                    />
                  </td>
                  <td>
                    <input
                      className="form-control form-control-sm text-end"
                      inputMode="decimal"
                      value={row.quantity}
                      onChange={(event) => recompute(index, { quantity: event.target.value })}
                    />
                  </td>
                  <td>
                    <input
                      className="form-control form-control-sm"
                      value={row.unit}
                      onChange={(event) => onChange(index, { unit: event.target.value })}
                    />
                  </td>
                  <td>
                    <input
                      className="form-control form-control-sm text-end"
                      inputMode="decimal"
                      value={row.unitPrice}
                      onChange={(event) => recompute(index, { unitPrice: event.target.value })}
                    />
                  </td>
                  <td>
                    <input
                      className="form-control form-control-sm text-end"
                      inputMode="decimal"
                      value={row.lineTotal}
                      onChange={(event) =>
                        onChange(index, { lineTotal: event.target.value, lineTotalManual: true })
                      }
                    />
                  </td>
                  <td>
                    <input
                      className="form-control form-control-sm text-end"
                      inputMode="decimal"
                      value={row.discount}
                      onChange={(event) => onChange(index, { discount: event.target.value })}
                      onBlur={(event) => {
                        // Rabat normalizujemy na ujemny, żeby nie trzeba było
                        // wbijać minusa przy każdej pozycji.
                        const cents = inputToCents(event.target.value);
                        if (cents) onChange(index, { discount: centsToInput(-Math.abs(cents)) });
                      }}
                    />
                  </td>
                  <td>
                    <EntityAutocomplete
                      value={row.productId === NEW_PRODUCT ? '' : row.productId}
                      options={productOptions}
                      onSelect={(id) => onChange(index, { productId: id || NO_PRODUCT })}
                      onCreate={(label) =>
                        onChange(index, { productId: NEW_PRODUCT, newProductName: label })
                      }
                      emptyOptionLabel="— pomiń pozycję —"
                      fallbackLabel={isNew ? row.newProductName || undefined : undefined}
                      placeholder="Wpisz, aby wyszukać produkt"
                      disabled={matching}
                    />
                    {isNew ? (
                      <div className="receipt-items__new-product">
                        <small className="receipt-items__new-product-hint">
                          Zostanie utworzony nowy produkt o tej nazwie
                        </small>
                        <TagPathInput
                          value={row.categoryPath}
                          categories={categories}
                          forAccounts={false}
                          placeholder="tag, np. jedzenie/nabiał"
                          onChange={(value) => onChange(index, { categoryPath: value })}
                        />
                      </div>
                    ) : null}
                  </td>
                  <td>
                    <Button
                      variant="link"
                      size="sm"
                      icon={<IconX size={16} />}
                      onClick={() => onRemove(index)}
                      aria-label="Usuń pozycję"
                    >
                      <span className="visually-hidden">Usuń pozycję</span>
                    </Button>
                  </td>
                </tr>
              );
            })}
            {!rows.length ? (
              <tr>
                <td colSpan={10} className="text-secondary text-center py-3">
                  Nie odczytano pozycji — dopisz je ręcznie albo zapisz sam paragon.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>

      <div className="receipt-items__footer">
        <Button variant="outline-secondary" size="sm" icon={<IconPlus size={16} />} onClick={onAdd}>
          pozycja
        </Button>
        <span className="flex-grow-1" />
        <span className="text-secondary">
          Σ pozycji: <strong>{money(balance)}</strong>
          {discounts ? ` (${money(gross)} − rabaty ${money(discounts)})` : ''}
        </span>
        {totalCents == null ? (
          <span className="receipt-badge receipt-badge--warn">brak sumy</span>
        ) : Math.abs(balance - totalCents) <= 1 ? (
          <span className="receipt-badge receipt-badge--ok">zgodne</span>
        ) : (
          <span className="receipt-badge receipt-badge--bad">
            różnica {money(balance - totalCents)}
          </span>
        )}
      </div>
    </div>
  );
};

export default ReceiptItemsTable;
