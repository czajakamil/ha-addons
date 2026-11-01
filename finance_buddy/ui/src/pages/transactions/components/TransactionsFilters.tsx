// Pasek filtrów: szukaj, zakres dat, konto, kategoria. Zmiany działają od razu; aktywne filtry to chipy.
import { useState } from 'react';
import { Search, X } from 'lucide-react';
import type { Account, Category } from '../../../api';
import { copy } from '../../../copy/pl';
import { formatShortDate } from '../../../utils/date';
import { datePresetRange, type DatePreset, type FilterState } from '../model';

const labels = copy.transactions;
const PRESETS = Object.keys(labels.presets) as DatePreset[];
const NO_FILTERS: FilterState = { startDate: '', endDate: '', accountId: '', categoryId: '' };

type TransactionsFiltersProps = {
  filters: FilterState;
  onChange: (filters: FilterState) => void;
  searchQuery: string;
  onSearchChange: (value: string) => void;
  activeAccounts: Account[];
  categories: Category[];
};

const TransactionsFilters = ({
  filters,
  onChange,
  searchQuery,
  onSearchChange,
  activeAccounts,
  categories
}: TransactionsFiltersProps) => {
  const [preset, setPreset] = useState<DatePreset>('all');

  const handlePresetChange = (next: DatePreset) => {
    setPreset(next);
    if (next !== 'custom') {
      onChange({ ...filters, ...datePresetRange(next) });
    }
  };

  const clearDates = () => {
    setPreset('all');
    onChange({ ...filters, startDate: '', endDate: '' });
  };

  const hasDates = Boolean(filters.startDate || filters.endDate);
  const dateChip =
    preset !== 'custom' && preset !== 'all'
      ? labels.presets[preset]
      : [filters.startDate && formatShortDate(filters.startDate), filters.endDate && formatShortDate(filters.endDate)]
          .filter(Boolean)
          .join(' – ');

  const chips = [
    hasDates ? { key: 'dates', label: dateChip, onRemove: clearDates } : null,
    filters.accountId
      ? {
          key: 'account',
          label: activeAccounts.find((account) => account.id === filters.accountId)?.name ?? labels.account,
          onRemove: () => onChange({ ...filters, accountId: '' })
        }
      : null,
    filters.categoryId
      ? {
          key: 'category',
          label: categories.find((category) => category.id === filters.categoryId)?.name ?? labels.category,
          onRemove: () => onChange({ ...filters, categoryId: '' })
        }
      : null
  ].filter((chip) => chip !== null);

  return (
    <div className="transactions-filters">
      <div className="transactions-filters__bar">
        <div className="transactions-filters__search">
          <Search size={16} aria-hidden="true" />
          <input
            type="search"
            className="form-control"
            aria-label={labels.searchLabel}
            placeholder={labels.search}
            value={searchQuery}
            onChange={(event) => onSearchChange(event.target.value)}
          />
        </div>
        <select
          className="form-select"
          aria-label={labels.dateRange}
          value={preset}
          onChange={(event) => handlePresetChange(event.target.value as DatePreset)}
        >
          {PRESETS.map((value) => (
            <option key={value} value={value}>
              {labels.presets[value]}
            </option>
          ))}
        </select>
        {preset === 'custom' && (
          <>
            <input
              type="date"
              className="form-control"
              aria-label={labels.dateFrom}
              value={filters.startDate}
              onChange={(event) => onChange({ ...filters, startDate: event.target.value })}
            />
            <input
              type="date"
              className="form-control"
              aria-label={labels.dateTo}
              value={filters.endDate}
              onChange={(event) => onChange({ ...filters, endDate: event.target.value })}
            />
          </>
        )}
        <select
          className="form-select"
          aria-label={labels.account}
          value={filters.accountId}
          onChange={(event) => onChange({ ...filters, accountId: event.target.value })}
        >
          <option value="">{labels.allAccounts}</option>
          {activeAccounts.map((account) => (
            <option key={account.id} value={account.id}>
              {account.name}
            </option>
          ))}
        </select>
        <select
          className="form-select"
          aria-label={labels.category}
          value={filters.categoryId}
          onChange={(event) => onChange({ ...filters, categoryId: event.target.value })}
        >
          <option value="">{labels.allCategories}</option>
          {categories.map((category) => (
            <option key={category.id} value={category.id}>
              {category.path || category.name}
            </option>
          ))}
        </select>
      </div>

      {chips.length > 0 && (
        <div className="transactions-filters__chips">
          {chips.map((chip) => (
            <button
              key={chip.key}
              type="button"
              className="filter-chip"
              aria-label={labels.removeFilter(chip.label)}
              onClick={chip.onRemove}
            >
              {chip.label}
              <X size={12} aria-hidden="true" />
            </button>
          ))}
          <button
            type="button"
            className="text-button"
            onClick={() => {
              setPreset('all');
              onChange(NO_FILTERS);
            }}
          >
            {labels.clearFilters}
          </button>
        </div>
      )}
    </div>
  );
};

export default TransactionsFilters;
