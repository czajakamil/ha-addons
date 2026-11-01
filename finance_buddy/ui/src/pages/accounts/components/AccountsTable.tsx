// Tabela kont jednej grupy: nazwa z podpisem, saldo, ostatnia aktywność. Akcje siedzą w menu „⋯".
import type { ReactNode } from 'react';
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import { Ellipsis } from 'lucide-react';
import type { Account } from '../../../api';
import TruncatedText from '../../../components/ui/TruncatedText';
import { copy } from '../../../copy/pl';
import { formatShortDate } from '../../../utils/date';
import { getAccountKindLabel, isLiabilityAccountKind } from '../model';

const labels = copy.accounts;

type AccountsTableProps = {
  accounts: Account[];
  activity: ReadonlyMap<string, string | null>;
  renderBalanceCell: (account: Account) => ReactNode;
  onOpen: (account: Account) => void;
  onEdit: (account: Account) => void;
  onChangeBalance: (account: Account) => void;
  onImport: (account: Account) => void;
  onDebt: (account: Account) => void;
};

const AccountsTable = ({
  accounts,
  activity,
  renderBalanceCell,
  onOpen,
  onEdit,
  onChangeBalance,
  onImport,
  onDebt
}: AccountsTableProps) => (
  <div className="accounts-table-card">
    <table className="accounts-table">
      <thead>
        <tr>
          <th scope="col">{labels.columns.name}</th>
          <th scope="col" className="accounts-table__amount">
            {labels.columns.balance}
          </th>
          <th scope="col" className="accounts-table__activity">
            {labels.columns.activity}
          </th>
          <th scope="col" className="accounts-table__menu" aria-label={labels.actions('')} />
        </tr>
      </thead>
      <tbody>
        {accounts.map((account) => {
          const lastActivity = activity.get(account.id);
          return (
            <tr key={account.id} className="accounts-table__row" onClick={() => onOpen(account)}>
              <td>
                <div className="accounts-name">
                  <span className="accounts-name__mark" aria-hidden="true">
                    {account.name.charAt(0).toUpperCase()}
                  </span>
                  <div className="accounts-name__copy">
                    <button
                      type="button"
                      className="accounts-name__link"
                      onClick={(event) => {
                        event.stopPropagation();
                        onOpen(account);
                      }}
                    >
                      <TruncatedText text={account.name} />
                    </button>
                    <span className="accounts-name__meta">
                      {getAccountKindLabel(account.account_kind)} · {account.currency}
                    </span>
                  </div>
                </div>
              </td>
              <td className="accounts-table__amount" onClick={(event) => event.stopPropagation()}>
                {renderBalanceCell(account)}
              </td>
              <td className="accounts-table__activity">
                {lastActivity ? formatShortDate(lastActivity) : lastActivity === null ? labels.noActivity : ''}
              </td>
              <td className="accounts-table__menu" onClick={(event) => event.stopPropagation()}>
                <DropdownMenu.Root>
                  <DropdownMenu.Trigger className="accounts-menu-trigger" aria-label={labels.actions(account.name)}>
                    <Ellipsis size={16} aria-hidden="true" />
                  </DropdownMenu.Trigger>
                  <DropdownMenu.Portal>
                    <DropdownMenu.Content className="menu" align="end" sideOffset={4}>
                      <DropdownMenu.Item className="menu__item" onSelect={() => onEdit(account)}>
                        {labels.edit}
                      </DropdownMenu.Item>
                      <DropdownMenu.Item className="menu__item" onSelect={() => onChangeBalance(account)}>
                        {labels.changeBalance}
                      </DropdownMenu.Item>
                      <DropdownMenu.Item className="menu__item" onSelect={() => onImport(account)}>
                        {labels.importCsv}
                      </DropdownMenu.Item>
                      {isLiabilityAccountKind(account.account_kind) && (
                        <DropdownMenu.Item className="menu__item" onSelect={() => onDebt(account)}>
                          {labels.debt}
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
  </div>
);

export default AccountsTable;
