// Pasek narzędzi listy przedmiotów: wyszukiwarka + wybór sortowania.
import type { Dispatch, SetStateAction } from 'react';
import { SearchIcon } from 'lucide-react';
import { SORT_OPTIONS, type SortOption } from '../model';

type ItemsToolbarRightProps = {
  searchQuery: string;
  setSearchQuery: Dispatch<SetStateAction<string>>;
  sortOption: SortOption;
  setSortOption: Dispatch<SetStateAction<SortOption>>;
};

const ItemsToolbarRight = ({
  searchQuery,
  setSearchQuery,
  sortOption,
  setSortOption
}: ItemsToolbarRightProps) => (
  <div className="accounts-toolbar">
    <div className="accounts-toolbar__field accounts-toolbar__field--search">
      <div className="input-icon">
        <span className="input-icon-addon">
          <SearchIcon size={16} aria-hidden="true" />
        </span>
        <input
          id="items-search-input"
          type="search"
          className="form-control form-control-sm accounts-search"
          placeholder="Nazwa, grupa, kategoria, producent…"
          value={searchQuery}
          onChange={(event) => setSearchQuery(event.target.value)}
          aria-label="Szukaj przedmiotów"
        />
      </div>
    </div>
    <div className="accounts-toolbar__field accounts-toolbar__field--sort">
      <select
        id="items-sort-select"
        className="form-select form-select-sm"
        value={sortOption}
        onChange={(event) => setSortOption(event.target.value as SortOption)}
        aria-label="Sortowanie przedmiotów"
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

export default ItemsToolbarRight;
