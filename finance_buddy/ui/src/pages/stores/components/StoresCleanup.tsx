// Widok „Do uporządkowania”: chipy powodów, tabela z zaznaczaniem i pasek akcji zbiorczych.
import { useEffect, useMemo, useState } from 'react';
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import { Ellipsis, GitMerge, SearchCheck } from 'lucide-react';
import type { Category } from '../../../api';
import EmptyState from '../../../components/ui/EmptyState';
import TruncatedText from '../../../components/ui/TruncatedText';
import { copy } from '../../../copy/pl';
import { maskAccountNumbers, type CleanupItem, type CleanupReason } from '../cleanup';
import type { StoreStat } from '../groups';
import CategoryDialog from './CategoryDialog';
import MergeDialog from './MergeDialog';

const labels = copy.stores;
const PAGE_SIZE = 100;
const REASONS: CleanupReason[] = ['duplicate', 'raw', 'self', 'uncategorized'];

const describeHint = (item: CleanupItem, stats: ReadonlyMap<string, StoreStat> | null) => {
  switch (item.reason) {
    case 'duplicate': {
      const other = item.duplicateOf;
      if (!other) return '';
      return labels.hints.duplicate(maskAccountNumbers(other.name), stats ? stats.get(other.id)?.count ?? 0 : null);
    }
    case 'raw':
      if (item.rawKind === 'digits') return labels.hints.digits;
      return item.rawKind === 'account' ? labels.hints.account(item.proposedName) : labels.hints.address(item.proposedName);
    case 'self':
      return labels.hints.self;
    default:
      return labels.hints.uncategorized;
  }
};

type StoresCleanupProps = {
  items: CleanupItem[];
  stats: ReadonlyMap<string, StoreStat> | null;
  categories: Category[];
  categoryMap: Map<string, Category>;
  onOpen: (storeId: string) => void;
  onArchive: (item: CleanupItem) => void;
  onSetCategory: (items: CleanupItem[], path: string) => Promise<boolean>;
};

const StoresCleanup = ({ items, stats, categories, categoryMap, onOpen, onArchive, onSetCategory }: StoresCleanupProps) => {
  const [reason, setReason] = useState<CleanupReason | 'all'>('all');
  const [selected, setSelected] = useState<ReadonlySet<string>>(new Set());
  const [limit, setLimit] = useState(PAGE_SIZE);
  const [dialog, setDialog] = useState<'merge' | 'category' | null>(null);
  const [categoryTargets, setCategoryTargets] = useState<CleanupItem[]>([]);

  // Sklep, który wypadł z listy (np. dostał kategorię), wypada też z zaznaczenia
  useEffect(() => {
    setSelected((current) => {
      const present = new Set(items.map((item) => item.store.id));
      const next = new Set([...current].filter((id) => present.has(id)));
      return next.size === current.size ? current : next;
    });
  }, [items]);

  const counts = useMemo(() => {
    const result = new Map<CleanupReason, number>();
    items.forEach((item) => result.set(item.reason, (result.get(item.reason) ?? 0) + 1));
    return result;
  }, [items]);

  const filtered = useMemo(
    () => (reason === 'all' ? items : items.filter((item) => item.reason === reason)),
    [items, reason]
  );
  const visible = filtered.slice(0, limit);
  const selectedItems = items.filter((item) => selected.has(item.store.id));
  const allSelected = filtered.length > 0 && filtered.every((item) => selected.has(item.store.id));

  const toggleOne = (id: string) =>
    setSelected((current) => {
      const next = new Set(current);
      if (!next.delete(id)) next.add(id);
      return next;
    });

  const toggleAll = () =>
    setSelected((current) => {
      const next = new Set(current);
      filtered.forEach((item) => (allSelected ? next.delete(item.store.id) : next.add(item.store.id)));
      return next;
    });

  const openCategory = (targets: CleanupItem[]) => {
    setCategoryTargets(targets);
    setDialog('category');
  };

  const firstCategoryId = selectedItems.find((item) => item.store.automatic_category_id)?.store.automatic_category_id;

  return (
    <div className="stores-cleanup">
      <div className="stores-chips" role="group" aria-label={labels.filters}>
        {(['all', ...REASONS] as const).map((value) => (
          <button
            key={value}
            type="button"
            aria-pressed={reason === value}
            className={`stores-chip${reason === value ? ' is-active' : ''}`}
            onClick={() => {
              setReason(value);
              setLimit(PAGE_SIZE);
            }}
          >
            {value === 'all' ? labels.filterAll : labels.reasons[value]}
            <span className="stores-chip__count">{value === 'all' ? items.length : counts.get(value) ?? 0}</span>
          </button>
        ))}
      </div>

      {selectedItems.length > 0 && (
        <div className="stores-bulk" role="toolbar" aria-label={labels.selected(selectedItems.length)}>
          <span className="stores-bulk__count">{labels.selected(selectedItems.length)}</span>
          <button
            type="button"
            className="stores-bulk__primary"
            disabled={selectedItems.length < 2}
            onClick={() => setDialog('merge')}
          >
            <GitMerge size={14} aria-hidden="true" />
            {labels.merge}
          </button>
          <button type="button" className="stores-bulk__action" onClick={() => openCategory(selectedItems)}>
            {labels.setCategory}
          </button>
          <button type="button" className="stores-bulk__clear" onClick={() => setSelected(new Set())}>
            {labels.clearSelection}
          </button>
        </div>
      )}

      {filtered.length === 0 ? (
        <EmptyState icon={SearchCheck} message={labels.cleanupEmpty} />
      ) : (
        <div className="accounts-table-card stores-table-card">
          <table className="accounts-table stores-table stores-table--cleanup">
            <thead>
              <tr>
                <th scope="col" className="stores-table__check">
                  <input
                    type="checkbox"
                    className="stores-check"
                    aria-label={labels.selectAll}
                    checked={allSelected}
                    onChange={toggleAll}
                  />
                </th>
                <th scope="col">{labels.columns.name}</th>
                <th scope="col" className="stores-table__reason">{labels.columns.reason}</th>
                <th scope="col" className="stores-table__hint">{labels.columns.hint}</th>
                <th scope="col" className="stores-table__count">{labels.columns.count}</th>
                <th scope="col" className="accounts-table__menu" aria-label={labels.actions('')} />
              </tr>
            </thead>
            <tbody>
              {visible.map((item) => {
                const name = maskAccountNumbers(item.store.name);
                const isSelected = selected.has(item.store.id);
                return (
                  <tr key={item.store.id} className={`stores-table__row${isSelected ? ' is-selected' : ''}`}>
                    <td className="stores-table__check">
                      <input
                        type="checkbox"
                        className="stores-check"
                        aria-label={labels.select(name)}
                        checked={isSelected}
                        onChange={() => toggleOne(item.store.id)}
                      />
                    </td>
                    <td>
                      <button type="button" className="accounts-name__link" onClick={() => onOpen(item.store.id)}>
                        <TruncatedText text={name} />
                      </button>
                      <span className={`stores-badge stores-badge--${item.reason} stores-badge--inline`}>
                        {labels.reasons[item.reason]}
                      </span>
                    </td>
                    <td className="stores-table__reason">
                      <span className={`stores-badge stores-badge--${item.reason}`}>{labels.reasons[item.reason]}</span>
                    </td>
                    <td className="stores-table__hint">
                      <TruncatedText text={describeHint(item, stats)} />
                    </td>
                    <td className="stores-table__count">{stats ? stats.get(item.store.id)?.count ?? 0 : '…'}</td>
                    <td className="accounts-table__menu">
                      <DropdownMenu.Root>
                        <DropdownMenu.Trigger className="accounts-menu-trigger stores-menu-visible" aria-label={labels.actions(name)}>
                          <Ellipsis size={16} aria-hidden="true" />
                        </DropdownMenu.Trigger>
                        <DropdownMenu.Portal>
                          <DropdownMenu.Content className="menu" align="end" sideOffset={4}>
                            <DropdownMenu.Item className="menu__item" onSelect={() => onOpen(item.store.id)}>
                              {labels.open}
                            </DropdownMenu.Item>
                            <DropdownMenu.Item className="menu__item" onSelect={() => openCategory([item])}>
                              {labels.setCategory}
                            </DropdownMenu.Item>
                            <DropdownMenu.Item className="menu__item" onSelect={() => onArchive(item)}>
                              {labels.archive}
                            </DropdownMenu.Item>
                          </DropdownMenu.Content>
                        </DropdownMenu.Portal>
                      </DropdownMenu.Root>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          <div className="stores-table-foot">
            <span>{labels.shown(visible.length, filtered.length)}</span>
            {visible.length < filtered.length && (
              <button type="button" className="text-button" onClick={() => setLimit((current) => current + PAGE_SIZE)}>
                {labels.showMore}
              </button>
            )}
          </div>
        </div>
      )}

      {dialog === 'merge' && (
        <MergeDialog
          items={selectedItems.map((item) => ({
            id: item.store.id,
            name: item.store.name,
            count: stats ? stats.get(item.store.id)?.count ?? 0 : null
          }))}
          categoryLabel={(firstCategoryId && categoryMap.get(firstCategoryId)?.name) || ''}
          onClose={() => setDialog(null)}
        />
      )}
      {dialog === 'category' && (
        <CategoryDialog
          count={categoryTargets.length}
          categories={categories}
          onClose={() => setDialog(null)}
          onConfirm={async (path) => {
            if (await onSetCategory(categoryTargets, path)) setDialog(null);
          }}
        />
      )}
    </div>
  );
};

export default StoresCleanup;
