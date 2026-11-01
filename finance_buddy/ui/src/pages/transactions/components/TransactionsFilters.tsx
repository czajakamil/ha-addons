// Formularz filtrów listy transakcji (zakres dat + konto).
import type { Dispatch, FormEvent, SetStateAction } from 'react';
import { Account } from '../../../api';
import DatePickerField from '../../../components/transactions/DatePickerField';
import { type FilterState } from '../model';

type TransactionsFiltersProps = {
  filters: FilterState;
  setFilters: Dispatch<SetStateAction<FilterState>>;
  activeAccounts: Account[];
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
};

const TransactionsFilters = ({ filters, setFilters, activeAccounts, onSubmit }: TransactionsFiltersProps) => (
  <div className="accounts-section-card__body transactions-filters">
    <form className="row g-3" onSubmit={onSubmit}>
      <DatePickerField
        id="transactions-start-date"
        label="Data od"
        placeholder="RRRR-MM-DD"
        value={filters.startDate}
        className="col-md-2"
        onChange={(value) => setFilters((prev) => ({ ...prev, startDate: value }))}
      />
      <DatePickerField
        id="transactions-end-date"
        label="Data do"
        placeholder="RRRR-MM-DD"
        value={filters.endDate}
        className="col-md-2"
        onChange={(value) => setFilters((prev) => ({ ...prev, endDate: value }))}
      />
      <div className="col-md-3">
        <label className="form-label">Konto</label>
        <select
          className="form-select"
          value={filters.accountId}
          onChange={(event) =>
            setFilters((prev) => ({ ...prev, accountId: event.target.value }))
          }
        >
          <option value="">Wszystkie konta</option>
          {activeAccounts.map((account) => (
            <option key={account.id} value={account.id}>
              {account.name}
            </option>
          ))}
        </select>
      </div>
      <div className="col-md-auto d-flex align-items-end">
        <button type="submit" className="btn btn-outline-primary is-focusable">
          Zastosuj
        </button>
      </div>
    </form>
  </div>
);

export default TransactionsFilters;
