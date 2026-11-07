// Widok „Do uporządkowania”: chipy powodów, tabela z zaznaczaniem i pasek akcji zbiorczych.
import { useEffect, useMemo, useState } from 'react';
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import { Ellipsis, GitMerge, SearchCheck, Trash2 } from 'lucide-react';
import type { Category } from '../../../api';
import EmptyState from '../../../components/ui/EmptyState';
import TruncatedText from '../../../components/ui/TruncatedText';
import { copy } from '../../../copy/pl';
import {
  buildMergePlans,
  maskAccountNumbers,
  type CategorySuggestion,
  type CleanupItem,
  type CleanupReason,
  type MergePlan
} from '../cleanup';
import type { StoreGroup, StoreStat } from '../groups';
import BulkMergeDialog from './BulkMergeDialog';
import CategoryDialog from './CategoryDialog';
import MergeDialog from './MergeDialog';
import SelfTransferDialog from './SelfTransferDialog';

const labels = copy.stores;
const PAGE_SIZE = 100;
const REASONS: CleanupReason[] = ['duplicate', 'raw', 'self', 'uncategorized'];

const describeHint = (
  item: CleanupItem,
  stats: ReadonlyMap<string, StoreStat> | null,
  suggestionPath: (item: CleanupItem) => string | null,
  suggestion: CategorySuggestion | undefined
) => {
  switch (item.reason) {
    case 'duplicate': {
      const other = item.duplicateOf;
      if (!other) return item.isAmbiguous && item.group.members.length > 1 ? labels.hints.sameNames : '';
      if (item.isAmbiguous) return labels.hints.ambiguous(maskAccountNumbers(other.name));
      return labels.hints.duplicate(maskAccountNumbers(other.name), stats ? other.count : null);
    }
    case 'raw':
      if (item.rawKind === 'digits') return labels.hints.digits;
      return item.rawKind === 'account' ? labels.hints.account(item.proposedName) : labels.hints.address(item.proposedName);
    case 'self':
      return labels.hints.self;
    default: {
      const path = suggestionPath(item);
      return path && suggestion
        ? labels.hints.suggested(path, suggestion.count, suggestion.total)
        : labels.hints.uncategorized;
    }
  }
};

type StoresCleanupProps = {
  items: CleanupItem[];
  stats: ReadonlyMap<string, StoreStat> | null;
  categories: Category[];
  categoryMap: Map<string, Category>;
  /** Podpowiedzi kategorii z historii transakcji, po kluczu grupy. */
  suggestions: ReadonlyMap<string, CategorySuggestion>;
  onApplySuggestions: (items: CleanupItem[]) => void;
  /** Potwierdza obecną nazwę grupy mimo reguły „surowej nazwy z banku”. */
  onVerifyNames: (items: CleanupItem[]) => void;
  onOpen: (storeId: string) => void;
  /** Archiwizuje całą grupę: wszystkie jej nazwy z banku. */
  onArchive: (item: CleanupItem) => void;
  /** Usuwa grupy bez transakcji (wszystkie ich nazwy z banku). Grupy z transakcjami trzeba scalić albo zarchiwizować. */
  onDelete: (groups: StoreGroup[]) => void;
  /** Wszystkie aktywne grupy bez transakcji, także te spoza tej listy. */
  emptyGroups: StoreGroup[];
  onSetCategory: (items: CleanupItem[], path: string) => Promise<boolean>;
  ensureCategory: (path: string) => Promise<string>;
  onMerged: () => Promise<void>;
};

const StoresCleanup = ({
  items,
  stats,
  categories,
  categoryMap,
  suggestions,
  onApplySuggestions,
  onVerifyNames,
  onOpen,
  onArchive,
  onDelete,
  emptyGroups,
  onSetCategory,
  ensureCategory,
  onMerged
}: StoresCleanupProps) => {
  const [reason, setReason] = useState<CleanupReason | 'all'>('all');
  const [selected, setSelected] = useState<ReadonlySet<string>>(new Set());
  const [limit, setLimit] = useState(PAGE_SIZE);
  const [dialog, setDialog] = useState<'merge' | 'category' | null>(null);
  const [categoryTargets, setCategoryTargets] = useState<CleanupItem[]>([]);
  const [transferStoreIds, setTransferStoreIds] = useState<string[] | null>(null);
  const [membersMerge, setMembersMerge] = useState<CleanupItem | null>(null);
  const [bulkPlans, setBulkPlans] = useState<MergePlan[] | null>(null);

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
  // Bez statystyk nie wiadomo, które grupy są puste, więc usuwanie czeka na ich wczytanie
  const isEmpty = (item: CleanupItem) => Boolean(stats) && item.group.count === 0;
  const selectedEmpty = selectedItems.filter(isEmpty);
  // Podpowiedź liczy się tylko, gdy jej kategoria wciąż istnieje
  const suggestionPath = (item: CleanupItem) => {
    const suggestion = suggestions.get(item.group.key);
    return (suggestion && categoryMap.get(suggestion.categoryId)?.path) || null;
  };
  const hasSuggestion = (item: CleanupItem) => item.reason === 'uncategorized' && Boolean(suggestionPath(item));
  const withSuggestion = items.filter(hasSuggestion);
  const selectedWithSuggestion = selectedItems.filter(hasSuggestion);
  const selectedRaw = selectedItems.filter((item) => item.reason === 'raw');
  const allMergePlans = useMemo(() => buildMergePlans(items), [items]);
  const selectedMergePlans = buildMergePlans(selectedItems);
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

  // Celem jest główny sklep grupy z największą liczbą transakcji, żeby przenieść jak najmniej.
  // Pozostałe nazwy z banku tej grupy i całe pozostałe grupy idą do niego.
  const mergeOrder = selectedItems.slice().sort((a, b) => b.group.count - a.group.count);
  const mergeTarget = mergeOrder[0]?.store;
  const mergeSourceIds = mergeOrder
    .flatMap((item) => item.group.members.map((member) => member.id))
    .filter((id) => id !== mergeTarget?.id);
  const mergeCategoryId = mergeOrder.find((item) => item.group.categoryId)?.group.categoryId;

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

      {(withSuggestion.length > 0 || allMergePlans.length > 0 || emptyGroups.length > 0) && (
        <div className="stores-quick-actions">
          {withSuggestion.length > 0 && (
            <button type="button" className="account-button" onClick={() => onApplySuggestions(withSuggestion)}>
              {labels.applySuggestions(withSuggestion.length)}
            </button>
          )}
          {allMergePlans.length > 0 && (
            <button type="button" className="account-button" onClick={() => setBulkPlans(allMergePlans)}>
              <GitMerge size={14} aria-hidden="true" />
              {labels.mergeAllDuplicates(allMergePlans.length)}
            </button>
          )}
          {emptyGroups.length > 0 && (
            <button type="button" className="account-button" onClick={() => onDelete(emptyGroups)}>
              <Trash2 size={14} aria-hidden="true" />
              {labels.deleteAllEmpty(emptyGroups.length)}
            </button>
          )}
        </div>
      )}

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
          {selectedMergePlans.length > 0 && (
            <button type="button" className="stores-bulk__action" onClick={() => setBulkPlans(selectedMergePlans)}>
              {labels.mergeSelectedDuplicates(selectedMergePlans.length)}
            </button>
          )}
          {selectedRaw.length > 0 && (
            <button type="button" className="stores-bulk__action" onClick={() => onVerifyNames(selectedRaw)}>
              {labels.verifyNames(selectedRaw.length)}
            </button>
          )}
          {selectedWithSuggestion.length > 0 && (
            <button
              type="button"
              className="stores-bulk__action"
              onClick={() => onApplySuggestions(selectedWithSuggestion)}
            >
              {labels.applySuggestions(selectedWithSuggestion.length)}
            </button>
          )}
          {selectedEmpty.length > 0 && (
            <button type="button" className="stores-bulk__action" onClick={() => onDelete(selectedEmpty.map((item) => item.group))}>
              {labels.deleteEmpty(selectedEmpty.length)}
            </button>
          )}
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
                const name = maskAccountNumbers(item.group.name);
                const members = item.group.members;
                // Pod nazwą grupy: nazwa z banku, gdy jest inna, albo liczba nazw z banku
                const bankName =
                  members.length > 1
                    ? labels.bankNames(members.length)
                    : members[0].name !== item.group.name
                      ? maskAccountNumbers(members[0].name)
                      : '';
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
                      <div className="accounts-name__copy">
                        <button type="button" className="accounts-name__link" onClick={() => onOpen(item.store.id)}>
                          <TruncatedText text={name} />
                        </button>
                        {bankName && (
                          <span className="accounts-name__meta">
                            <TruncatedText text={bankName} />
                          </span>
                        )}
                      </div>
                      <span className={`stores-badge stores-badge--${item.reason} stores-badge--inline`}>
                        {labels.reasons[item.reason]}
                      </span>
                    </td>
                    <td className="stores-table__reason">
                      <span className={`stores-badge stores-badge--${item.reason}`}>{labels.reasons[item.reason]}</span>
                    </td>
                    <td className="stores-table__hint">
                      <TruncatedText text={describeHint(item, stats, suggestionPath, suggestions.get(item.group.key))} />
                    </td>
                    <td className="stores-table__count">{stats ? item.group.count : '…'}</td>
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
                            {item.reason === 'raw' && (
                              <DropdownMenu.Item className="menu__item" onSelect={() => onVerifyNames([item])}>
                                {labels.verifyName}
                              </DropdownMenu.Item>
                            )}
                            {hasSuggestion(item) && (
                              <DropdownMenu.Item className="menu__item" onSelect={() => onApplySuggestions([item])}>
                                {labels.applySuggestion}
                              </DropdownMenu.Item>
                            )}
                            {members.length > 1 && (
                              <DropdownMenu.Item className="menu__item" onSelect={() => setMembersMerge(item)}>
                                {labels.mergeMembers}
                              </DropdownMenu.Item>
                            )}
                            <DropdownMenu.Item className="menu__item" onSelect={() => openCategory([item])}>
                              {labels.setCategory}
                            </DropdownMenu.Item>
                            <DropdownMenu.Item className="menu__item" onSelect={() => setTransferStoreIds(members.map((member) => member.id))}>
                              {labels.transfers.menu}
                            </DropdownMenu.Item>
                            <DropdownMenu.Item className="menu__item" onSelect={() => onArchive(item)}>
                              {labels.archive}
                            </DropdownMenu.Item>
                            {isEmpty(item) && (
                              <DropdownMenu.Item className="menu__item" onSelect={() => onDelete([item.group])}>
                                {labels.deleteStore}
                              </DropdownMenu.Item>
                            )}
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

      {dialog === 'merge' && mergeTarget && (
        <MergeDialog
          sourceIds={mergeSourceIds}
          targetId={mergeTarget.id}
          defaultName={mergeOrder[0].group.name}
          names={mergeOrder.map((item) => item.group.name)}
          categories={categories}
          categoryPath={(mergeCategoryId && categoryMap.get(mergeCategoryId)?.path) || ''}
          ensureCategory={ensureCategory}
          onClose={() => setDialog(null)}
          onMerged={async () => {
            setDialog(null);
            await onMerged();
          }}
          onUndone={onMerged}
        />
      )}
      {bulkPlans && (
        <BulkMergeDialog
          plans={bulkPlans}
          onClose={() => setBulkPlans(null)}
          onMerged={async () => {
            setSelected(new Set());
            await onMerged();
          }}
        />
      )}
      {membersMerge && (
        <MergeDialog
          sourceIds={membersMerge.group.members.map((member) => member.id).filter((id) => id !== membersMerge.store.id)}
          targetId={membersMerge.store.id}
          defaultName={membersMerge.group.name}
          names={membersMerge.group.members.map((member) => member.name)}
          categories={categories}
          categoryPath={(membersMerge.group.categoryId && categoryMap.get(membersMerge.group.categoryId)?.path) || ''}
          ensureCategory={ensureCategory}
          onClose={() => setMembersMerge(null)}
          onMerged={async () => {
            setMembersMerge(null);
            await onMerged();
          }}
          onUndone={onMerged}
        />
      )}
      {transferStoreIds && (
        <SelfTransferDialog storeIds={transferStoreIds} onClose={() => setTransferStoreIds(null)} onChanged={onMerged} />
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
