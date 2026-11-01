// Pasek narzędzi listy sklepów: wyszukiwarka + wybór sortowania.
import type { Dispatch, SetStateAction } from 'react';
import { IconSearch } from '@tabler/icons-react';
import { SORT_OPTIONS, type SortOption } from '../model';

type StoresToolbarRightProps = {
  searchQuery: string;
  setSearchQuery: Dispatch<SetStateAction<string>>;
  sortOption: SortOption;
  setSortOption: Dispatch<SetStateAction<SortOption>>;
};

const StoresToolbarRight = ({
  searchQuery,
  setSearchQuery,
  sortOption,
  setSortOption
}: StoresToolbarRightProps) => (
  <div className="accounts-toolbar">
    <div className="accounts-toolbar__field accounts-toolbar__field--search">
      <div className="input-icon">
        <span className="input-icon-addon">
          <IconSearch size={16} aria-hidden="true" />
        </span>
        <input
          id="stores-search-input"
          type="search"
          className="form-control form-control-sm accounts-search"
          placeholder="Nazwa, grupa, NIP, adres…"
          value={searchQuery}
          onChange={(event) => setSearchQuery(event.target.value)}
          aria-label="Szukaj sklepów"
        />
      </div>
    </div>
    <div className="accounts-toolbar__field accounts-toolbar__field--sort">
      <select
        id="stores-sort-select"
        className="form-select form-select-sm"
        value={sortOption}
        onChange={(event) => setSortOption(event.target.value as SortOption)}
        aria-label="Sortowanie sklepów"
      >
        {SORT_OPTIONS.map(({ value, label }) => (
          <option key={value} value={value}>
            {label}
          </option>
        ))}
      </select>
    </div>
    {searchQuery.trim().length > 0 && (
      <button
        type="button"
        className="btn btn-sm btn-outline-secondary is-focusable accounts-toolbar__clear"
        onClick={() => setSearchQuery('')}
      >
        Wyczyść
      </button>
    )}
  </div>
);

export default StoresToolbarRight;
