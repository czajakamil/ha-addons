// Tabela kont jednej grupy: nazwa z podpisem, saldo, ostatnia aktywność. Akcje siedzą w menu „⋯".
// Dwuklik na nazwie zmienia ją w miejscu, więc pojedynczy klik czeka chwilę, zanim otworzy konto.
import { useEffect, useRef, useState } from 'react';
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import { Ellipsis } from 'lucide-react';
import type { Account } from '../../../api';
import Money from '../../../components/ui/Money';
import TruncatedText from '../../../components/ui/TruncatedText';
import { copy } from '../../../copy/pl';
import { formatShortDate } from '../../../utils/date';
import { getAccountKindLabel, isLiabilityAccountKind } from '../model';
import ResizableTable from '../../../components/ui/ResizableTable';
import AccountAvatar from './AccountAvatar';
import AccountNameInput from './AccountNameInput';

const labels = copy.accounts;
const DOUBLE_CLICK_MS = 250;

type AccountsTableProps = {
  accounts: Account[];
  activity: ReadonlyMap<string, string | null>;
  onOpen: (account: Account) => void;
  onSettings: (account: Account) => void;
  onReconcile: (account: Account) => void;
  onImport: (account: Account) => void;
  onToggleArchived: (account: Account) => void;
  onRename: (account: Account, name: string) => void;
  onDebt: (account: Account) => void;
};

const AccountsTable = ({
  accounts,
  activity,
  onOpen,
  onSettings,
  onReconcile,
  onImport,
  onToggleArchived,
  onRename,
  onDebt
}: AccountsTableProps) => {
  const [renamingId, setRenamingId] = useState<string | null>(null);
  const openTimerRef = useRef<number | undefined>(undefined);
  useEffect(() => () => window.clearTimeout(openTimerRef.current), []);

  return (
  <div className="accounts-table-card">
    <ResizableTable tableId="accounts" className="accounts-table">
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
                  {/* Konta bez wybranej ikony i koloru (np. z importu) zostają przy literze, żeby się różniły */}
                  {account.icon || account.color ? (
                    <AccountAvatar icon={account.icon} color={account.color} size="sm" />
                  ) : (
                    <span className="accounts-name__mark" aria-hidden="true">
                      {account.name.charAt(0).toUpperCase()}
                    </span>
                  )}
                  <div className="accounts-name__copy">
                    {renamingId === account.id ? (
                      <AccountNameInput
                        name={account.name}
                        className="accounts-name__input"
                        onCancel={() => setRenamingId(null)}
                        onSave={(name) => {
                          setRenamingId(null);
                          onRename(account, name);
                        }}
                      />
                    ) : (
                      <button
                        type="button"
                        className="accounts-name__link"
                        onClick={(event) => {
                          event.stopPropagation();
                          // Klawiatura (detail 0) nie ma dwukliku, więc otwiera od razu
                          if (event.detail === 0) {
                            onOpen(account);
                            return;
                          }
                          window.clearTimeout(openTimerRef.current);
                          if (event.detail === 1) {
                            openTimerRef.current = window.setTimeout(() => onOpen(account), DOUBLE_CLICK_MS);
                          }
                        }}
                        onDoubleClick={(event) => {
                          event.stopPropagation();
                          window.clearTimeout(openTimerRef.current);
                          setRenamingId(account.id);
                        }}
                      >
                        <TruncatedText text={account.name} />
                      </button>
                    )}
                    <span className="accounts-name__meta">
                      {getAccountKindLabel(account.account_kind)} · {account.currency}
                    </span>
                  </div>
                </div>
              </td>
              <td className="accounts-table__amount" onClick={(event) => event.stopPropagation()}>
                <Money
                  cents={account.balance_cents}
                  currency={account.currency}
                  tone={account.balance_cents < 0 ? 'negative' : 'neutral'}
                />
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
                      <DropdownMenu.Item className="menu__item" onSelect={() => onReconcile(account)}>
                        {labels.reconcile}
                      </DropdownMenu.Item>
                      <DropdownMenu.Item className="menu__item" onSelect={() => onImport(account)}>
                        {labels.importCsv}
                      </DropdownMenu.Item>
                      <DropdownMenu.Item className="menu__item" onSelect={() => onToggleArchived(account)}>
                        {account.is_closed ? labels.restore : labels.archive}
                      </DropdownMenu.Item>
                      {isLiabilityAccountKind(account.account_kind) && (
                        <DropdownMenu.Item className="menu__item" onSelect={() => onDebt(account)}>
                          {labels.debt}
                        </DropdownMenu.Item>
                      )}
                      <DropdownMenu.Separator className="menu__separator" />
                      <DropdownMenu.Item className="menu__item" onSelect={() => onSettings(account)}>
                        {labels.settingsLink}
                      </DropdownMenu.Item>
                    </DropdownMenu.Content>
                  </DropdownMenu.Portal>
                </DropdownMenu.Root>
              </td>
            </tr>
          );
        })}
      </tbody>
    </ResizableTable>
  </div>
  );
};

export default AccountsTable;
