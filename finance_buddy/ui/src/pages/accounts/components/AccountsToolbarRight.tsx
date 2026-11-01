// Pasek narzędzi listy kont: wyszukiwarka + wybór sortowania.
import type { Dispatch, SetStateAction } from 'react';
import { Search } from 'lucide-react';
import { SORT_OPTIONS, type SortOption } from '../model';

type AccountsToolbarRightProps = {
  searchQuery: string;
  setSearchQuery: Dispatch<SetStateAction<string>>;
  sortOption: SortOption;
  setSortOption: Dispatch<SetStateAction<SortOption>>;
};

const AccountsToolbarRight = ({
  searchQuery,
  setSearchQuery,
  sortOption,
  setSortOption
}: AccountsToolbarRightProps) => (
  <div className="accounts-toolbar">
    <div className="accounts-toolbar__field accounts-toolbar__field--search">
      <div className="input-icon">
        <span className="input-icon-addon">
          <Search size={16} aria-hidden="true" />
        </span>
        <input
          id="accounts-search-input"
          type="search"
          className="form-control form-control-sm accounts-search"
          placeholder="Nazwa, waluta, kategoria…"
          value={searchQuery}
          onChange={(event) => setSearchQuery(event.target.value)}
          aria-label="Szukaj kont"
        />
      </div>
    </div>
    <div className="accounts-toolbar__field accounts-toolbar__field--sort">
      <select
        id="accounts-sort-select"
        className="form-select form-select-sm"
        value={sortOption}
        onChange={(event) => setSortOption(event.target.value as SortOption)}
        aria-label="Sortowanie kont"
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

export default AccountsToolbarRight;
