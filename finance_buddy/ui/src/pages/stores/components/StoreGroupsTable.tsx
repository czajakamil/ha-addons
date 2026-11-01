// Lista sklepów: wiersz na grupę, rozwinięcie pokazuje nazwy z banku. Akcje siedzą w „⋯”.
// Dwuklik na nazwie zmienia ją w miejscu, więc pojedynczy klik czeka chwilę, zanim otworzy sklep.
import { Fragment, useEffect, useRef, useState } from 'react';
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import { ChevronDown, ChevronRight, Ellipsis } from 'lucide-react';
import type { Category } from '../../../api';
import Money from '../../../components/ui/Money';
import ResizableTable from '../../../components/ui/ResizableTable';
import TruncatedText from '../../../components/ui/TruncatedText';
import { copy } from '../../../copy/pl';
import { formatShortDate } from '../../../utils/date';
import AccountNameInput from '../../accounts/components/AccountNameInput';
import { maskAccountNumbers } from '../cleanup';
import type { StoreGroup, StoreStat } from '../groups';
import StoreCategory from './StoreCategory';

const labels = copy.stores;
const DOUBLE_CLICK_MS = 250;

type StoreGroupsTableProps = {
  groups: StoreGroup[];
  stats: ReadonlyMap<string, StoreStat> | null;
  hiddenKeys: ReadonlySet<string>;
  categoryMap: Map<string, Category>;
  onOpen: (group: StoreGroup) => void;
  onSettings: (group: StoreGroup) => void;
  onRename: (group: StoreGroup, name: string) => void;
  onToggleArchived: (group: StoreGroup) => void;
};

const StoreGroupsTable = ({
  groups,
  stats,
  hiddenKeys,
  categoryMap,
  onOpen,
  onSettings,
  onRename,
  onToggleArchived
}: StoreGroupsTableProps) => {
  const [expanded, setExpanded] = useState<ReadonlySet<string>>(new Set());
  const [renamingKey, setRenamingKey] = useState<string | null>(null);
  const openTimerRef = useRef<number | undefined>(undefined);
  useEffect(() => () => window.clearTimeout(openTimerRef.current), []);

  const shown = (key: string) => !hiddenKeys.has(key);
  const columnCount = 3 + ['category', 'count', 'spent', 'last', 'nip', 'address'].filter(shown).length;

  const toggle = (key: string) =>
    setExpanded((current) => {
      const next = new Set(current);
      if (!next.delete(key)) next.add(key);
      return next;
    });

  return (
    <div className="accounts-table-card stores-table-card">
      <ResizableTable tableId="stores-groups" className="accounts-table stores-table">
        <thead>
          <tr>
            <th scope="col" className="stores-table__toggle" aria-hidden="true" />
            <th scope="col" data-col="store">{labels.columns.store}</th>
            {shown('category') && <th scope="col" className="stores-table__category">{labels.columns.category}</th>}
            {shown('nip') && <th scope="col" className="stores-table__extra">{labels.columns.nip}</th>}
            {shown('address') && <th scope="col" className="stores-table__extra">{labels.columns.address}</th>}
            {shown('count') && <th scope="col" className="stores-table__count">{labels.columns.count}</th>}
            {shown('spent') && <th scope="col" className="stores-table__spent">{labels.columns.spent}</th>}
            {shown('last') && <th scope="col" className="stores-table__last">{labels.columns.last}</th>}
            <th scope="col" className="accounts-table__menu" aria-label={labels.actions('')} />
          </tr>
        </thead>
        <tbody>
          {groups.map((group) => {
            const multi = group.members.length > 1;
            const isExpanded = expanded.has(group.key);
            const displayName = maskAccountNumbers(group.name);
            return (
              <Fragment key={group.key}>
                <tr className="accounts-table__row" onClick={() => onOpen(group)}>
                  <td className="stores-table__toggle" onClick={(event) => event.stopPropagation()}>
                    {multi && (
                      <button
                        type="button"
                        className="stores-toggle"
                        aria-expanded={isExpanded}
                        aria-label={isExpanded ? labels.collapse(displayName) : labels.expand(displayName)}
                        onClick={() => toggle(group.key)}
                      >
                        {isExpanded ? <ChevronDown size={16} aria-hidden="true" /> : <ChevronRight size={16} aria-hidden="true" />}
                      </button>
                    )}
                  </td>
                  <td>
                    <div className="accounts-name">
                      <span className="stores-mark" aria-hidden="true">
                        {displayName.charAt(0).toUpperCase()}
                      </span>
                      <div className="accounts-name__copy">
                        {renamingKey === group.key ? (
                          <AccountNameInput
                            name={group.name}
                            className="accounts-name__input"
                            ariaLabel={labels.rename(displayName)}
                            tooShortMessage={labels.nameTooShort}
                            onCancel={() => setRenamingKey(null)}
                            onSave={(name) => {
                              setRenamingKey(null);
                              onRename(group, name);
                            }}
                          />
                        ) : (
                          <button
                            type="button"
                            className="accounts-name__link"
                            title={labels.renameHint}
                            onClick={(event) => {
                              event.stopPropagation();
                              // Klawiatura (detail 0) nie ma dwukliku, więc otwiera od razu
                              if (event.detail === 0) {
                                onOpen(group);
                                return;
                              }
                              window.clearTimeout(openTimerRef.current);
                              if (event.detail === 1) {
                                openTimerRef.current = window.setTimeout(() => onOpen(group), DOUBLE_CLICK_MS);
                              }
                            }}
                            onDoubleClick={(event) => {
                              event.stopPropagation();
                              window.clearTimeout(openTimerRef.current);
                              setRenamingKey(group.key);
                            }}
                          >
                            <TruncatedText text={displayName} />
                          </button>
                        )}
                        {multi && <span className="accounts-name__meta">{labels.bankNames(group.members.length)}</span>}
                      </div>
                    </div>
                  </td>
                  {shown('category') && (
                    <td className="stores-table__category">
                      <StoreCategory categoryId={group.categoryId} categoryMap={categoryMap} />
                    </td>
                  )}
                  {shown('nip') && <td className="stores-table__extra">{group.members.find((store) => store.nip)?.nip ?? ''}</td>}
                  {shown('address') && (
                    <td className="stores-table__extra">
                      <TruncatedText text={group.members.find((store) => store.address)?.address ?? ''} />
                    </td>
                  )}
                  {shown('count') && <td className="stores-table__count">{stats ? group.count : '…'}</td>}
                  {shown('spent') && (
                    <td className="stores-table__spent">
                      {stats ? <Money cents={group.spentCents} tone="neutral" /> : '…'}
                    </td>
                  )}
                  {shown('last') && (
                    <td className="stores-table__last">
                      {stats ? (group.lastDate ? formatShortDate(group.lastDate) : '—') : '…'}
                    </td>
                  )}
                  <td className="accounts-table__menu" onClick={(event) => event.stopPropagation()}>
                    <DropdownMenu.Root>
                      <DropdownMenu.Trigger className="accounts-menu-trigger" aria-label={labels.actions(displayName)}>
                        <Ellipsis size={16} aria-hidden="true" />
                      </DropdownMenu.Trigger>
                      <DropdownMenu.Portal>
                        <DropdownMenu.Content className="menu" align="end" sideOffset={4}>
                          <DropdownMenu.Item className="menu__item" onSelect={() => onOpen(group)}>
                            {labels.open}
                          </DropdownMenu.Item>
                          <DropdownMenu.Item className="menu__item" onSelect={() => setRenamingKey(group.key)}>
                            {labels.rename('').trim()}
                          </DropdownMenu.Item>
                          <DropdownMenu.Item className="menu__item" onSelect={() => onToggleArchived(group)}>
                            {group.isArchived ? labels.restore : labels.archive}
                          </DropdownMenu.Item>
                          <DropdownMenu.Separator className="menu__separator" />
                          <DropdownMenu.Item className="menu__item" onSelect={() => onSettings(group)}>
                            {labels.settingsLink}
                          </DropdownMenu.Item>
                        </DropdownMenu.Content>
                      </DropdownMenu.Portal>
                    </DropdownMenu.Root>
                  </td>
                </tr>
                {multi && isExpanded && (
                  <tr className="stores-table__variants">
                    <td colSpan={columnCount}>
                      <ul className="stores-variants">
                        {group.members.map((member) => (
                          <li key={member.id} className="stores-variants__row">
                            <span className="stores-variants__tag">{labels.fromBank}</span>
                            <span className="stores-list__name">{maskAccountNumbers(member.name)}</span>
                            {stats && (
                              <span className="stores-list__count">
                                {labels.transactionsCount(stats.get(member.id)?.count ?? 0)}
                              </span>
                            )}
                          </li>
                        ))}
                      </ul>
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </ResizableTable>
    </div>
  );
};

export default StoreGroupsTable;
