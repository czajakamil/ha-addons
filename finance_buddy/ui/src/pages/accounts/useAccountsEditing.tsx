// Edycja inline wierszy kont: nazwa/waluta/komentarz, kategoria (autouzupełnianie),
// saldo (z modalem wyrównania), typ konta i status budżetu. Zwraca funkcje
// renderujące komórki oraz handler dwukliku siatki, używane przez AccountsPage.
import { useCallback, useEffect, useRef, useState, type ChangeEvent, type KeyboardEvent as ReactKeyboardEvent } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import { type CellClickArgs } from 'react-data-grid';
import {
  Account,
  AccountBalanceAdjustmentPayload,
  AccountKind,
  AccountUpdate,
  Category,
  adjustAccountBalance,
  updateAccount
} from '../../api';
import TagEditCell from '../../components/tags/TagEditCell';
import { useTagCellEditing } from '../../components/tags/useTagCellEditing';
import { formatMoney, parseAmountToCents } from '../../utils/format';
import { captureGridCellAnchor, refocusGridCell } from '../../utils/gridFocus';
import AccountAvatar from './components/AccountAvatar';
import {
  NAVIGATION_KEYS,
  focusClosestGridCell,
  getAccountKindLabel,
  isAccountColumnKey,
  isCreditAccountKind,
  isEditableFieldColumn,
  stopNavigationKeyPropagation,
  type AccountKindOption,
  type BalanceAdjustmentModalState,
  type BudgetLabelValue,
  type EditableField,
  type EditingCell
} from './model';

type UseAccountsEditingParams = {
  accounts: Account[];
  setAccounts: Dispatch<SetStateAction<Account[] | undefined>>;
  loadAccounts: () => Promise<Account[] | undefined>;
  accountKindOptions: AccountKindOption[];
  categories: Category[];
  getCategoryLabel: (categoryId?: string | null) => string;
  ensureAccountCategory: (label: string) => Promise<string>;
};

export const useAccountsEditing = ({
  accounts,
  setAccounts,
  loadAccounts,
  accountKindOptions,
  categories,
  getCategoryLabel,
  ensureAccountCategory
}: UseAccountsEditingParams) => {
  const [editingCell, setEditingCell] = useState<EditingCell | null>(null);
  const [savingCell, setSavingCell] = useState<EditingCell | null>(null);
  const [editValue, setEditValue] = useState('');
  const [inlineError, setInlineError] = useState<string | null>(null);
  const skipNextCommitRef = useRef(false);
  const [balanceEditingAccountId, setBalanceEditingAccountId] = useState<string | null>(null);
  const [balanceEditValue, setBalanceEditValue] = useState('');
  const [balanceAdjustmentModal, setBalanceAdjustmentModal] = useState<BalanceAdjustmentModalState | null>(null);
  const [adjustingBalance, setAdjustingBalance] = useState(false);
  const [budgetEditingAccountId, setBudgetEditingAccountId] = useState<string | null>(null);
  const [budgetEditValue, setBudgetEditValue] = useState<BudgetLabelValue>('on');
  const [budgetSavingAccountId, setBudgetSavingAccountId] = useState<string | null>(null);
  const [kindEditingAccountId, setKindEditingAccountId] = useState<string | null>(null);
  const [kindEditValue, setKindEditValue] = useState<AccountKind>('cash');
  const [kindSavingAccountId, setKindSavingAccountId] = useState<string | null>(null);

  const categoryEditing = useTagCellEditing<Account>({
    entities: accounts,
    getCurrentCategoryId: (account) => account.category_id ?? null,
    getInitialValue: (account) => getCategoryLabel(account.category_id),
    resolveCategoryId: ensureAccountCategory,
    save: async (account, nextCategoryId) => {
      const updated = await updateAccount(account.id, { category_id: nextCategoryId });
      setAccounts((prev) => (prev ?? []).map((acc) => (acc.id === updated.id ? updated : acc)));
    },
    setError: setInlineError,
    errorMessages: {
      resolve: 'Nie udało się utworzyć tagu konta. Spróbuj ponownie.',
      save: 'Nie udało się zaktualizować tagu konta. Spróbuj ponownie.'
    }
  });

  const startEditing = useCallback(
    (account: Account, field: EditableField) => {
      if (savingCell && savingCell.accountId === account.id && savingCell.field === field) {
        return;
      }
      skipNextCommitRef.current = false;
      const currentValue =
        field === 'comment' ? account.comment ?? '' : field === 'name' ? account.name : account.currency;
      setEditingCell({ accountId: account.id, field });
      setEditValue(currentValue);
      setInlineError(null);
    },
    [savingCell]
  );

  const startBalanceEditing = useCallback((account: Account) => {
    setBalanceEditingAccountId(account.id);
    setBalanceEditValue((account.balance_cents / 100).toFixed(2));
    setInlineError(null);
  }, []);

  const startBudgetEditing = useCallback(
    (account: Account) => {
      if (budgetSavingAccountId) return;
      setBudgetEditingAccountId(account.id);
      setBudgetEditValue(account.is_on_budget ? 'on' : 'off');
      setInlineError(null);
    },
    [budgetSavingAccountId]
  );

  const cancelBudgetEditing = () => {
    if (budgetSavingAccountId) return;
    setBudgetEditingAccountId(null);
  };

  const handleBudgetToggle = async (account: Account) => {
    if (budgetSavingAccountId) return;
    const nextIsOnBudget = !account.is_on_budget;
    try {
      setBudgetSavingAccountId(account.id);
      const updated = await updateAccount(account.id, { is_on_budget: nextIsOnBudget });
      setAccounts((prev) => (prev ?? []).map((acc) => (acc.id === updated.id ? updated : acc)));
      setInlineError(null);
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się zmienić statusu budżetu. Spróbuj ponownie.');
    } finally {
      setBudgetSavingAccountId(null);
    }
  };

  const handleBudgetLabelChange = async (value: BudgetLabelValue) => {
    if (!budgetEditingAccountId) return;
    const account = accounts.find((acc) => acc.id === budgetEditingAccountId);
    if (!account) return;

    // Commit dzieje się w onChange selecta — fokusowany select zaraz zniknie,
    // więc kotwicę do przywrócenia fokusu łapiemy z aktywnego elementu.
    const anchor = captureGridCellAnchor(document.activeElement);
    const nextIsOnBudget = value === 'on';
    setBudgetEditValue(value);

    if (account.is_on_budget === nextIsOnBudget) {
      setBudgetEditingAccountId(null);
      refocusGridCell(anchor);
      return;
    }

    try {
      setBudgetSavingAccountId(account.id);
      const updated = await updateAccount(account.id, { is_on_budget: nextIsOnBudget });
      setAccounts((prev) => (prev ?? []).map((acc) => (acc.id === updated.id ? updated : acc)));
      setInlineError(null);
      setBudgetEditingAccountId(null);
      refocusGridCell(anchor);
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się zmienić statusu budżetu. Spróbuj ponownie.');
    } finally {
      setBudgetSavingAccountId(null);
    }
  };

  const startKindEditing = useCallback(
    (account: Account) => {
      if (kindSavingAccountId) return;
      setKindEditingAccountId(account.id);
      setKindEditValue(account.account_kind);
      setInlineError(null);
    },
    [kindSavingAccountId]
  );

  const cancelKindEditing = () => {
    if (kindSavingAccountId) return;
    setKindEditingAccountId(null);
  };

  const handleKindChange = async (value: AccountKind) => {
    if (!kindEditingAccountId) return;
    const account = accounts.find((acc) => acc.id === kindEditingAccountId);
    if (!account) return;

    const anchor = captureGridCellAnchor(document.activeElement);
    setKindEditValue(value);
    if (account.account_kind === value) {
      setKindEditingAccountId(null);
      refocusGridCell(anchor);
      return;
    }

    try {
      setKindSavingAccountId(account.id);
      const updated = await updateAccount(account.id, { account_kind: value });
      setAccounts((prev) => (prev ?? []).map((acc) => (acc.id === updated.id ? updated : acc)));
      setInlineError(null);
      setKindEditingAccountId(null);
      refocusGridCell(anchor);
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się zmienić typu konta. Spróbuj ponownie.');
    } finally {
      setKindSavingAccountId(null);
    }
  };

  const cancelBalanceEditing = () => {
    setBalanceEditingAccountId(null);
    setBalanceEditValue('');
  };

  const commitBalanceEdit = (account: Account) => {
    const nextValueCents = parseAmountToCents(balanceEditValue);
    if (nextValueCents === null) {
      setInlineError('Podaj poprawną kwotę salda (np. 123.45).');
      return;
    }

    if (nextValueCents === account.balance_cents) {
      cancelBalanceEditing();
      return;
    }

    setBalanceAdjustmentModal({
      account,
      targetBalanceCents: nextValueCents
    });
    cancelBalanceEditing();
  };

  const cancelEditing = () => {
    skipNextCommitRef.current = true;
    setEditingCell(null);
    setEditValue('');
  };

  const commitEdit = async () => {
    if (skipNextCommitRef.current) {
      skipNextCommitRef.current = false;
      return;
    }

    if (!editingCell || savingCell) return;

    const account = accounts.find((acc) => acc.id === editingCell.accountId);
    if (!account) return;

    const field = editingCell.field;
    let nextValue = field === 'comment' ? editValue : editValue.trim();
    if (field === 'currency') {
      nextValue = nextValue.toUpperCase();
    }

    if (field === 'name' && nextValue.length < 2) {
      setInlineError('Nazwa konta musi mieć co najmniej 2 znaki.');
      return;
    }

    if (field === 'currency' && !/^[A-Z]{3}$/.test(nextValue)) {
      setInlineError('Waluta musi mieć format ISO 4217 (np. PLN, USD).');
      return;
    }

    const currentValue =
      field === 'comment' ? account.comment ?? '' : field === 'name' ? account.name : account.currency;

    if (currentValue === nextValue) {
      cancelEditing();
      return;
    }

    const payload: AccountUpdate = {};

    if (field === 'comment') {
      payload.comment = nextValue.length ? nextValue : null;
    } else if (field === 'name') {
      payload.name = nextValue;
    } else {
      payload.currency = nextValue;
    }

    setSavingCell(editingCell);
    try {
      const updated = await updateAccount(account.id, payload);
      setAccounts((prev) => (prev ?? []).map((acc) => (acc.id === updated.id ? updated : acc)));
      setInlineError(null);
      cancelEditing();
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się zapisać zmian. Spróbuj ponownie.');
    } finally {
      setSavingCell(null);
    }
  };

  const handleEditableInputKeyDown = (
    event: ReactKeyboardEvent<HTMLInputElement | HTMLTextAreaElement>,
    field: EditableField
  ) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      cancelEditing();
      focusClosestGridCell(event.target);
      return;
    }

    if (event.key === 'Enter') {
      if (field === 'comment' && !(event.metaKey || event.ctrlKey)) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      // Kotwica przed commitem — po nim input jest odmontowany i closest()
      // nie znajdzie już komórki, przez co fokus spadłby na <body>.
      const anchor = captureGridCellAnchor(event.target);
      void (async () => {
        await commitEdit();
        refocusGridCell(anchor);
      })();
      return;
    }

    if (NAVIGATION_KEYS.has(event.key)) {
      // W trybie edycji strzałki powinny działać w polu edytora, nie w gridzie.
      event.stopPropagation();
    }
  };

  const handleBalanceKeyDown = (event: ReactKeyboardEvent<HTMLInputElement>, account: Account) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      cancelBalanceEditing();
      focusClosestGridCell(event.target);
      return;
    }
    if (event.key === 'Enter') {
      event.preventDefault();
      event.stopPropagation();
      commitBalanceEdit(account);
      focusClosestGridCell(event.target);
      return;
    }

    if (NAVIGATION_KEYS.has(event.key)) {
      event.stopPropagation();
    }
  };

  const handleBudgetKeyDown = (event: ReactKeyboardEvent<HTMLSelectElement>) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      cancelBudgetEditing();
      focusClosestGridCell(event.target);
      return;
    }

    if (NAVIGATION_KEYS.has(event.key)) {
      event.stopPropagation();
    }
  };

  const handleKindKeyDown = (event: ReactKeyboardEvent<HTMLSelectElement>) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      cancelKindEditing();
      focusClosestGridCell(event.target);
      return;
    }

    if (NAVIGATION_KEYS.has(event.key)) {
      event.stopPropagation();
    }
  };

  const handleCancelBalanceAdjustment = () => {
    if (adjustingBalance) return;
    setBalanceAdjustmentModal(null);
  };

  const handleConfirmBalanceAdjustment = async () => {
    if (!balanceAdjustmentModal) return;
    const payload: AccountBalanceAdjustmentPayload = {
      target_balance_cents: balanceAdjustmentModal.targetBalanceCents
    };
    try {
      setAdjustingBalance(true);
      await adjustAccountBalance(balanceAdjustmentModal.account.id, payload);
      await loadAccounts();
      setBalanceAdjustmentModal(null);
      setInlineError(null);
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się wyrównać salda. Spróbuj ponownie.');
    } finally {
      setAdjustingBalance(false);
    }
  };

  const renderCell = (account: Account, field: EditableField) => {
    const isEditing = editingCell?.accountId === account.id && editingCell.field === field;
    const isSaving = savingCell?.accountId === account.id && savingCell.field === field;
    const cellClass = ['editable-cell', 'align-middle', isEditing ? 'is-editing' : ''].filter(Boolean).join(' ');

    if (isEditing) {
      const commonProps = {
        autoFocus: true,
        value: editValue,
        disabled: isSaving,
        className: 'form-control form-control-sm',
        onChange: (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
          const incomingValue = event.target.value;
          setEditValue(field === 'currency' ? incomingValue.toUpperCase() : incomingValue);
        },
        onBlur: () => void commitEdit()
      };

      return (
        <div className={cellClass}>
          {field === 'comment' ? (
            <textarea
              rows={3}
              {...commonProps}
              onKeyDownCapture={stopNavigationKeyPropagation}
              onKeyDown={(event) => handleEditableInputKeyDown(event, field)}
            />
          ) : (
            <input
              type="text"
              maxLength={field === 'currency' ? 3 : undefined}
              {...commonProps}
              onKeyDownCapture={stopNavigationKeyPropagation}
              onKeyDown={(event) => handleEditableInputKeyDown(event, field)}
            />
          )}
          <div className="editable-cell__meta">
            {isSaving ? 'Zapisywanie…' : field === 'comment' ? 'Cmd/Ctrl + Enter aby zapisać' : 'Enter aby zapisać'}
          </div>
        </div>
      );
    }

    const rawValue = field === 'comment' ? account.comment ?? '' : field === 'name' ? account.name : account.currency;
    const isPlaceholder = !rawValue;
    const valueClass = ['editable-cell__value', isPlaceholder ? 'editable-cell__value--placeholder' : '']
      .filter(Boolean)
      .join(' ');
    const displayValue = rawValue || '—';

    return (
      <div className={cellClass} title="Dwuklik, aby edytować">
        {field === 'name' ? (
          <span className="account-name-cell">
            <AccountAvatar icon={account.icon} color={account.color} size="sm" />
            <span className={valueClass}>{displayValue}</span>
          </span>
        ) : (
          <span className={valueClass}>{displayValue}</span>
        )}
      </div>
    );
  };

  const renderCategoryCell = (account: Account) => (
    <TagEditCell
      isEditing={categoryEditing.editingId === account.id}
      isSaving={categoryEditing.savingId === account.id}
      displayValue={getCategoryLabel(account.category_id)}
      categoryId={account.category_id}
      editValue={categoryEditing.editValue}
      categories={categories}
      forAccounts
      placeholder="np. inwestycje/obligacje"
      onChange={categoryEditing.setEditValue}
      onCommit={(value) => void categoryEditing.commitEdit(value)}
      onCancel={categoryEditing.cancelEditing}
    />
  );

  const renderBalanceCell = (account: Account) => {
    const isEditingBalance = balanceEditingAccountId === account.id;
    const cellClass = [
      'editable-cell',
      'align-middle',
      'fw-semibold',
      'text-end',
      isEditingBalance ? 'is-editing' : 'justify-content-end'
    ]
      .filter(Boolean)
      .join(' ');

    if (isEditingBalance) {
      return (
        <div className={cellClass}>
          <input
            type="text"
            className="form-control form-control-sm text-end"
            autoFocus
            value={balanceEditValue}
            onChange={(event: ChangeEvent<HTMLInputElement>) => setBalanceEditValue(event.target.value)}
            onBlur={() => commitBalanceEdit(account)}
            onKeyDown={(event) => handleBalanceKeyDown(event, account)}
            inputMode="decimal"
          />
          <div className="editable-cell__meta text-end">Enter aby kontynuować</div>
        </div>
      );
    }

    return (
      <div className={cellClass} title="Dwuklik, aby edytować saldo">
        {formatMoney(account.balance_cents, account.currency)}
      </div>
    );
  };

  const renderKindCell = (account: Account) => {
    const isEditingKind = kindEditingAccountId === account.id;
    const isSavingKind = kindSavingAccountId === account.id;
    const cellClass = [
      'editable-cell',
      'align-middle',
      'text-center',
      isEditingKind ? 'is-editing' : '',
      !isEditingKind && 'd-flex',
      !isEditingKind && 'justify-content-center',
      !isEditingKind && 'align-items-center'
    ]
      .filter(Boolean)
      .join(' ');
    const nonEditingCellStyle = !isEditingKind ? { height: '100%', width: '100%' } : undefined;
    const label = getAccountKindLabel(account.account_kind);
    const badgeClass = isCreditAccountKind(account.account_kind)
      ? 'accounts-status-badge accounts-status-badge--warning'
      : 'accounts-status-badge accounts-status-badge--info';

    if (isEditingKind) {
      return (
        <div className={cellClass}>
          <select
            className="form-select form-select-sm text-center"
            autoFocus
            value={kindEditValue}
            onChange={(event: ChangeEvent<HTMLSelectElement>) =>
              void handleKindChange(event.target.value as AccountKind)
            }
            onBlur={cancelKindEditing}
            disabled={isSavingKind}
            onKeyDown={handleKindKeyDown}
          >
            {accountKindOptions.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
          <div className="editable-cell__meta">
            {isSavingKind ? 'Zapisywanie…' : 'Wybierz typ konta'}
          </div>
        </div>
      );
    }

    return (
      <div className={cellClass} style={nonEditingCellStyle} title="Dwuklik, aby zmienić typ konta">
        <span className={badgeClass} title={label}>
          {label}
        </span>
      </div>
    );
  };

  const renderBudgetCell = (account: Account) => {
    const isEditingBudget = budgetEditingAccountId === account.id;
    const isSavingBudget = budgetSavingAccountId === account.id;
    const cellClass = [
      'editable-cell',
      'align-middle',
      'text-center',
      isEditingBudget ? 'is-editing' : '',
      !isEditingBudget && 'd-flex',
      !isEditingBudget && 'justify-content-center',
      !isEditingBudget && 'align-items-center'
    ]
      .filter(Boolean)
      .join(' ');
    const nonEditingCellStyle = !isEditingBudget ? { height: '100%', width: '100%' } : undefined;
    const budgetLabel = account.is_on_budget ? 'Budżet' : 'Śledzenie';
    const budgetStatusClass = account.is_on_budget
      ? 'accounts-status-badge accounts-status-badge--success'
      : 'accounts-status-badge accounts-status-badge--neutral';

    if (isEditingBudget) {
      return (
        <div className={cellClass}>
          <select
            className="form-select form-select-sm text-center"
            autoFocus
            value={budgetEditValue}
            onChange={(event: ChangeEvent<HTMLSelectElement>) =>
              void handleBudgetLabelChange(event.target.value as BudgetLabelValue)
            }
            onBlur={cancelBudgetEditing}
            disabled={isSavingBudget}
            onKeyDown={handleBudgetKeyDown}
          >
            <option value="on">Budżet</option>
            <option value="off">Śledzenie</option>
          </select>
          <div className="editable-cell__meta">
            {isSavingBudget ? 'Zapisywanie…' : 'Wybierz status konta względem budżetu'}
          </div>
        </div>
      );
    }

    return (
      <div className={cellClass} style={nonEditingCellStyle} title="Kliknij, aby zmienić status budżetu">
        <button
          type="button"
          className="accounts-budget-toggle is-focusable"
          onClick={(event) => {
            event.preventDefault();
            event.stopPropagation();
            void handleBudgetToggle(account);
          }}
          disabled={isSavingBudget}
          aria-pressed={account.is_on_budget}
          aria-label={`Przełącz status konta: ${budgetLabel}`}
        >
          <span className={budgetStatusClass}>{budgetLabel}</span>
        </button>
      </div>
    );
  };

  const handleGridDoubleClick = useCallback(
    (args: CellClickArgs<Account>) => {
      const columnKey = String(args.column.key);
      if (!isAccountColumnKey(columnKey)) {
        return;
      }
      if (columnKey === 'balance') {
        if (balanceEditingAccountId !== args.row.id) {
          startBalanceEditing(args.row);
        }
        return;
      }
      if (columnKey === 'category') {
        if (categoryEditing.editingId !== args.row.id && !categoryEditing.savingId) {
          categoryEditing.startEditing(args.row);
        }
        return;
      }
      if (columnKey === 'budget') {
        if (budgetEditingAccountId !== args.row.id && !budgetSavingAccountId) {
          startBudgetEditing(args.row);
        }
        return;
      }
      if (columnKey === 'kind') {
        if (kindEditingAccountId !== args.row.id && !kindSavingAccountId) {
          startKindEditing(args.row);
        }
        return;
      }
      if (isEditableFieldColumn(columnKey)) {
        if (editingCell?.accountId === args.row.id && editingCell.field === columnKey) {
          return;
        }
        startEditing(args.row, columnKey);
      }
    },
    [
      balanceEditingAccountId,
      categoryEditing,
      budgetEditingAccountId,
      budgetSavingAccountId,
      kindEditingAccountId,
      kindSavingAccountId,
      editingCell,
      startBalanceEditing,
      startBudgetEditing,
      startKindEditing,
      startEditing
    ]
  );

  // Podczas edycji komórki klawisze nawigacji nie mogą trafiać do react-data-grid.
  // (Edycję tagu obsługuje analogiczny efekt wewnątrz useTagCellEditing.)
  useEffect(() => {
    const isEditingSomething =
      editingCell !== null ||
      balanceEditingAccountId !== null ||
      budgetEditingAccountId !== null ||
      kindEditingAccountId !== null;

    if (!isEditingSomething) {
      return;
    }

    const handleKeyDownCapture = (event: KeyboardEvent) => {
      if (!NAVIGATION_KEYS.has(event.key)) {
        return;
      }

      const activeElement = document.activeElement as HTMLElement | null;
      if (!activeElement) return;

      const editableWrapper = activeElement.closest('.editable-cell');
      if (!editableWrapper) return;

      event.stopPropagation();
      event.stopImmediatePropagation();
    };

    window.addEventListener('keydown', handleKeyDownCapture, true);

    return () => {
      window.removeEventListener('keydown', handleKeyDownCapture, true);
    };
  }, [editingCell, balanceEditingAccountId, budgetEditingAccountId, kindEditingAccountId]);

  return {
    inlineError,
    setInlineError,
    renderCell,
    renderCategoryCell,
    renderBalanceCell,
    renderKindCell,
    renderBudgetCell,
    handleGridDoubleClick,
    balanceAdjustmentModal,
    adjustingBalance,
    handleCancelBalanceAdjustment,
    handleConfirmBalanceAdjustment
  };
};
