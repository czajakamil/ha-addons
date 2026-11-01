import { IconSearch } from '@tabler/icons-react';

type TransactionsToolbarRightProps = {
  searchQuery: string;
  setSearchQuery: (value: string) => void;
};

const TransactionsToolbarRight = ({ searchQuery, setSearchQuery }: TransactionsToolbarRightProps) => (
  <div className="accounts-toolbar accounts-toolbar--search-only">
    <div className="accounts-toolbar__field accounts-toolbar__field--search">
      <label className="accounts-toolbar__label" htmlFor="transactions-search-input">
        Szukaj
      </label>
      <div className="input-icon">
        <span className="input-icon-addon">
          <IconSearch size={16} aria-hidden="true" />
        </span>
        <input
          id="transactions-search-input"
          type="search"
          className="form-control form-control-sm accounts-search"
          placeholder="Sklep, komentarz, tag, kwota…"
          value={searchQuery}
          onChange={(event) => setSearchQuery(event.target.value)}
        />
      </div>
    </div>
  </div>
);

export default TransactionsToolbarRight;
