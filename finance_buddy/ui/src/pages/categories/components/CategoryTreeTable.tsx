// Drzewo kategorii jako tabela: kwoty gałęzi, mini-wykres miesięcy i akcje w „⋯”.
// Wiersz można przeciągnąć: górna i dolna ćwiartka wstawiają obok, środek — do środka.
import { useRef, useState, type DragEvent, type ReactNode } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import { ArrowDown, ArrowUp, ChevronDown, ChevronRight, Ellipsis, GripVertical } from 'lucide-react';
import type { Category } from '../../../api';
import { resolveTagColor } from '../../../components/tags/tagColors';
import Money from '../../../components/ui/Money';
import TruncatedText from '../../../components/ui/TruncatedText';
import { copy } from '../../../copy/pl';
import { formatShortDate } from '../../../utils/date';
import AccountNameInput from '../../accounts/components/AccountNameInput';
import type { CategorySort, CategorySortKey, DropPosition, VisibleRow } from '../model';
import { formatCategoryPath } from '../../../utils/categoryPath';

const labels = copy.categories;
const SPARK_HEIGHT_PX = 18;
const INDENT_PX = 20;

const Sparkline = ({ values, color }: { values: readonly number[]; color: string }) => {
  const visible = values.map((value) => Math.max(0, value));
  const peak = Math.max(...visible, 1);
  const empty = visible.every((value) => value === 0);
  return (
    <span className="categories-spark" aria-hidden="true">
      {visible.map((value, index) => (
        <span
          key={index}
          style={{
            height: `${Math.max(1, Math.round((value / peak) * SPARK_HEIGHT_PX))}px`,
            background: color,
            // Bieżący miesiąc jest niepełny, a puste gałęzie nie mają trendu
            opacity: empty ? 0.25 : index === visible.length - 1 ? 0.35 : 1
          }}
        />
      ))}
    </span>
  );
};

type SortableHeaderProps = {
  column: CategorySortKey;
  sort: CategorySort;
  onSort: (column: CategorySortKey) => void;
  className?: string;
  children: ReactNode;
};

const SortableHeader = ({ column, sort, onSort, className, children }: SortableHeaderProps) => {
  const active = sort?.key === column;
  const Icon = sort?.direction === 'asc' ? ArrowUp : ArrowDown;
  return (
    <th
      scope="col"
      className={className}
      aria-sort={active ? (sort?.direction === 'asc' ? 'ascending' : 'descending') : undefined}
    >
      <button type="button" className={`stores-sort${active ? ' stores-sort--active' : ''}`} onClick={() => onSort(column)}>
        {children}
        {active && <Icon size={14} aria-hidden="true" className="stores-sort__icon" />}
      </button>
    </th>
  );
};

const dropPositionAt = (event: DragEvent<HTMLElement>): DropPosition => {
  const rect = event.currentTarget.getBoundingClientRect();
  const ratio = (event.clientY - rect.top) / Math.max(rect.height, 1);
  if (ratio < 0.25) return 'before';
  if (ratio > 0.75) return 'after';
  return 'inside';
};

type CategoryTreeTableProps = {
  rows: VisibleRow[];
  sort: CategorySort;
  onSort: (column: CategorySortKey) => void;
  categoryMap: Map<string, Category>;
  statsLoaded: boolean;
  renamingId: string | null;
  onRenameStart: (id: string | null) => void;
  onRename: (category: Category, name: string) => void;
  onToggle: (id: string) => void;
  onAddChild: (category: Category) => void;
  onMove: (category: Category) => void;
  onShift: (category: Category, delta: -1 | 1) => void;
  onDelete: (row: VisibleRow) => void;
  onDrop: (draggedId: string, targetId: string, position: DropPosition) => void;
};

const CategoryTreeTable = ({
  rows,
  sort,
  onSort,
  categoryMap,
  statsLoaded,
  renamingId,
  onRenameStart,
  onRename,
  onToggle,
  onAddChild,
  onMove,
  onShift,
  onDelete,
  onDrop
}: CategoryTreeTableProps) => {
  const navigate = useNavigate();
  const draggedRef = useRef<string | null>(null);
  const [dropTarget, setDropTarget] = useState<{ id: string; position: DropPosition } | null>(null);

  const open = (category: Category) => navigate(`/categories/${category.id}`);
  // Przy sortowaniu po kolumnie kolejność wynika z danych, więc przeciąganie jej nie zmieni
  const manualOrder = sort === null;

  return (
    <div className="accounts-table-card categories-table-card">
      <div className="categories-table__scroll">
        <table className="accounts-table categories-table">
          <thead>
            <tr>
              <th scope="col" className="categories-table__handle" aria-hidden="true" />
              <SortableHeader column="name" sort={sort} onSort={onSort}>
                {labels.columns.category}
              </SortableHeader>
              <SortableHeader column="count" sort={sort} onSort={onSort} className="categories-table__num categories-table__count">
                {labels.columns.count}
              </SortableHeader>
              <SortableHeader column="spent" sort={sort} onSort={onSort} className="categories-table__num categories-table__spent">
                {labels.columns.spent}
              </SortableHeader>
              <SortableHeader column="last" sort={sort} onSort={onSort} className="categories-table__num categories-table__last">
                {labels.columns.last}
              </SortableHeader>
              <th scope="col" className="categories-table__num categories-table__trend">{labels.columns.trend}</th>
              <th scope="col" className="accounts-table__menu" aria-label={labels.actions('')} />
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const { category, depth, total } = row;
              const color = resolveTagColor(category, categoryMap).value;
              const isTarget = dropTarget?.id === category.id;
              const classes = [
                'accounts-table__row',
                'categories-row',
                depth === 0 ? 'categories-row--root' : '',
                isTarget ? `categories-row--drop-${dropTarget.position}` : ''
              ].filter(Boolean);
              const isEmpty = statsLoaded && total.count === 0;
              return (
                <tr
                  key={category.id}
                  className={classes.join(' ')}
                  onClick={() => open(category)}
                  onDragOver={(event) => {
                    const dragged = draggedRef.current;
                    if (!manualOrder || !dragged || dragged === category.id) return;
                    event.preventDefault();
                    event.dataTransfer.dropEffect = 'move';
                    const position = dropPositionAt(event);
                    if (dropTarget?.id !== category.id || dropTarget.position !== position) {
                      setDropTarget({ id: category.id, position });
                    }
                  }}
                  onDragLeave={(event) => {
                    if (!event.currentTarget.contains(event.relatedTarget as Node | null)) {
                      setDropTarget((current) => (current?.id === category.id ? null : current));
                    }
                  }}
                  onDrop={(event) => {
                    event.preventDefault();
                    const dragged = draggedRef.current;
                    const position = dropPositionAt(event);
                    draggedRef.current = null;
                    setDropTarget(null);
                    if (dragged) onDrop(dragged, category.id, position);
                  }}
                >
                  <td className="categories-table__handle" onClick={(event) => event.stopPropagation()}>
                    {manualOrder && (
                      <span
                        className="categories-handle"
                        draggable
                        title={labels.dragHandle(category.name)}
                        onDragStart={(event) => {
                          draggedRef.current = category.id;
                          event.dataTransfer.effectAllowed = 'move';
                          event.dataTransfer.setData('text/plain', category.id);
                          const row = event.currentTarget.closest('tr');
                          if (row) event.dataTransfer.setDragImage(row, 16, 16);
                        }}
                        onDragEnd={() => {
                          draggedRef.current = null;
                          setDropTarget(null);
                        }}
                      >
                        <GripVertical size={14} aria-hidden="true" />
                      </span>
                    )}
                  </td>
                  <td>
                    <div className="categories-name" style={{ paddingInlineStart: depth * INDENT_PX }}>
                      {row.hasChildren ? (
                        <button
                          type="button"
                          className="stores-toggle categories-name__toggle"
                          aria-expanded={row.expanded}
                          aria-label={row.expanded ? labels.collapse(category.name) : labels.expand(category.name)}
                          onClick={(event) => {
                            event.stopPropagation();
                            onToggle(category.id);
                          }}
                        >
                          {row.expanded ? <ChevronDown size={16} aria-hidden="true" /> : <ChevronRight size={16} aria-hidden="true" />}
                        </button>
                      ) : (
                        <span className="categories-name__spacer" aria-hidden="true" />
                      )}
                      <span className="tag-dot" style={{ ['--tag-color' as string]: color }} aria-hidden="true" />
                      {renamingId === category.id ? (
                        <span onClick={(event) => event.stopPropagation()} className="categories-name__edit">
                          <AccountNameInput
                            name={category.name}
                            className="accounts-name__input"
                            ariaLabel={labels.rename(category.name)}
                            tooShortMessage={labels.nameTooShort}
                            onCancel={() => onRenameStart(null)}
                            onSave={(name) => {
                              onRenameStart(null);
                              onRename(category, name);
                            }}
                          />
                        </span>
                      ) : (
                        <Link
                          to={`/categories/${category.id}`}
                          className="categories-name__link"
                          title={formatCategoryPath(category.path)}
                          onClick={(event) => event.stopPropagation()}
                        >
                          <TruncatedText text={category.name} />
                        </Link>
                      )}
                      {row.children.length > 0 && <span className="categories-name__count">{row.children.length}</span>}
                    </div>
                  </td>
                  <td className="categories-table__num categories-table__count stores-muted">{statsLoaded ? total.count : '…'}</td>
                  <td className={`categories-table__num categories-table__spent${isEmpty ? ' stores-muted' : ''}`}>
                    {statsLoaded ? <Money cents={total.spentCents} tone="neutral" /> : '…'}
                  </td>
                  <td className="categories-table__num categories-table__last stores-muted">
                    {statsLoaded ? (total.lastDate ? formatShortDate(total.lastDate) : '—') : '…'}
                  </td>
                  <td className="categories-table__num categories-table__trend">
                    {statsLoaded && <Sparkline values={total.monthly} color={color} />}
                  </td>
                  <td className="accounts-table__menu" onClick={(event) => event.stopPropagation()}>
                    <DropdownMenu.Root>
                      <DropdownMenu.Trigger className="accounts-menu-trigger" aria-label={labels.actions(category.name)}>
                        <Ellipsis size={16} aria-hidden="true" />
                      </DropdownMenu.Trigger>
                      <DropdownMenu.Portal>
                        <DropdownMenu.Content className="menu" align="end" sideOffset={4}>
                          <DropdownMenu.Item className="menu__item" onSelect={() => onRenameStart(category.id)}>
                            {labels.menu.rename}
                          </DropdownMenu.Item>
                          <DropdownMenu.Item className="menu__item" onSelect={() => onAddChild(category)}>
                            {labels.menu.addChild}
                          </DropdownMenu.Item>
                          <DropdownMenu.Item className="menu__item" onSelect={() => onMove(category)}>
                            {labels.menu.move}
                          </DropdownMenu.Item>
                          <DropdownMenu.Item className="menu__item" disabled={!manualOrder} onSelect={() => onShift(category, -1)}>
                            {labels.menu.moveUp}
                          </DropdownMenu.Item>
                          <DropdownMenu.Item className="menu__item" disabled={!manualOrder} onSelect={() => onShift(category, 1)}>
                            {labels.menu.moveDown}
                          </DropdownMenu.Item>
                          <DropdownMenu.Item
                            className="menu__item"
                            onSelect={() => navigate(`/transactions?category=${encodeURIComponent(category.id)}`)}
                          >
                            {labels.menu.transactions}
                          </DropdownMenu.Item>
                          <DropdownMenu.Separator className="menu__separator" />
                          <DropdownMenu.Item
                            className="menu__item menu__item--danger"
                            disabled={!isEmpty}
                            onSelect={() => onDelete(row)}
                          >
                            {labels.menu.delete}
                          </DropdownMenu.Item>
                          {!isEmpty && <div className="categories-menu__hint">{labels.menu.deleteOnlyEmpty}</div>}
                        </DropdownMenu.Content>
                      </DropdownMenu.Portal>
                    </DropdownMenu.Root>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default CategoryTreeTable;
