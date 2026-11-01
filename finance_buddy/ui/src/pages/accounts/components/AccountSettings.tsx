// Zakładka Ustawienia. Ikona, kolor i tag zapisują się od razu (z „Cofnij”).
// Status, waluta i typ zmieniają budżet lub sens kwot, więc najpierw pokazujemy skutek.
import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { toast } from 'sonner';
import { type Account, type AccountUpdate, type Category, deleteAccount } from '../../../api';
import TagPathInput from '../../../components/tags/TagPathInput';
import SegmentedControl from '../../../components/ui/SegmentedControl';
import { copy } from '../../../copy/pl';
import { DEFAULT_ACCOUNT_COLOR, DEFAULT_ACCOUNT_ICON } from '../accountPresentation';
import { ACCOUNT_KIND_OPTIONS, TOP_CURRENCIES, currencyOptions, getAccountKindLabel } from '../model';
import { describeBudgetStatusChange, describeCurrencyChange, describeKindChange } from '../settingsEffects';
import AccountIconPicker from './AccountIconPicker';
import AccountSelect, { type AccountSelectOption } from './AccountSelect';

const labels = copy.accounts;
const text = labels.settings;

const toCurrencyOption = ({ code, name }: { code: string; name: string }): AccountSelectOption => ({
  value: code,
  keywords: [name],
  label: (
    <>
      <b>{code}</b> {name}
    </>
  )
});

const currencyGroups = [
  TOP_CURRENCIES.map((code) => currencyOptions.find((option) => option.code === code)!).map(toCurrencyOption),
  currencyOptions
    .filter((option) => !TOP_CURRENCIES.includes(option.code))
    .sort((a, b) => a.code.localeCompare(b.code))
    .map(toCurrencyOption)
];

const kindGroups = [ACCOUNT_KIND_OPTIONS.map(({ value, label, description }) => ({ value, label, description }))];

type PendingChange = { field: 'status' | 'currency' | 'kind'; payload: AccountUpdate; effect: string };

type AccountSettingsProps = {
  account: Account;
  categories: Category[];
  tagLabel: string;
  ensureCategory: (path: string) => Promise<string>;
  save: (
    account: Account,
    payload: AccountUpdate,
    options?: { message?: string; undoable?: boolean }
  ) => Promise<boolean>;
};

const TagField = ({
  account,
  categories,
  tagLabel,
  ensureCategory,
  save
}: Pick<AccountSettingsProps, 'account' | 'categories' | 'tagLabel' | 'ensureCategory' | 'save'>) => {
  const [value, setValue] = useState(tagLabel);

  const commit = async (path: string) => {
    const next = path.trim();
    if (next === tagLabel) return;
    try {
      await save(account, { category_id: next ? await ensureCategory(next) : null });
    } catch (err) {
      console.error(err);
      toast.error(text.tagFailed);
      setValue(tagLabel);
    }
  };

  return (
    <TagPathInput
      inputId="account-setting-tag"
      value={value}
      categories={categories}
      forAccounts
      placeholder={text.tagPlaceholder}
      onChange={setValue}
      onCommit={(path) => void commit(path)}
      onCancel={() => setValue(tagLabel)}
    />
  );
};

const AccountSettings = ({ account, categories, tagLabel, ensureCategory, save }: AccountSettingsProps) => {
  const navigate = useNavigate();
  const [pending, setPending] = useState<PendingChange | null>(null);
  const [saving, setSaving] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [confirmation, setConfirmation] = useState('');
  const [deleting, setDeleting] = useState(false);

  const confirmPending = async () => {
    if (!pending) return;
    setSaving(true);
    // Bez „Cofnij”: cofnięcie zmieniłoby budżet bez pokazania skutku.
    const saved = await save(account, pending.payload, { undoable: false });
    setSaving(false);
    if (saved) setPending(null);
  };

  const renderConfirmation = (field: PendingChange['field']) =>
    pending?.field === field && (
      <div className="account-confirm" role="group" aria-label={text.confirmChange}>
        <p>{pending.effect}</p>
        <div className="account-actions account-actions--start">
          <button
            type="button"
            className="account-button account-button--primary"
            disabled={saving}
            onClick={() => void confirmPending()}
          >
            {text.confirmChange}
          </button>
          <button type="button" className="account-button" disabled={saving} onClick={() => setPending(null)}>
            {text.cancelChange}
          </button>
        </div>
      </div>
    );

  const currency = pending?.payload.currency ?? account.currency;
  const kind = pending?.payload.account_kind ?? account.account_kind;
  const isOnBudget = pending?.payload.is_on_budget ?? account.is_on_budget;
  const currencyName = currencyOptions.find((option) => option.code === currency)?.name;

  const handleDelete = async () => {
    try {
      setDeleting(true);
      await deleteAccount(account.id);
      toast(text.deleted);
      navigate('/accounts');
    } catch (err) {
      console.error(err);
      toast.error(text.deleteFailed);
      setDeleting(false);
    }
  };

  return (
    <div className="account-settings">
      <section className="account-settings__card" aria-label={labels.tabs.settings}>
        <div className="account-settings__row">
          <span className="account-field-label">{text.iconColor}</span>
          <AccountIconPicker
            icon={account.icon ?? DEFAULT_ACCOUNT_ICON}
            color={account.color ?? DEFAULT_ACCOUNT_COLOR}
            onIconChange={(icon) => void save(account, { icon })}
            onColorChange={(color) => void save(account, { color })}
          />
        </div>

        <div className="account-settings__row">
          <label className="account-field-label" htmlFor="account-setting-tag">
            {text.tag}
            <span className="account-hint">{text.tagHint}</span>
          </label>
          <TagField
            key={tagLabel}
            account={account}
            categories={categories}
            tagLabel={tagLabel}
            ensureCategory={ensureCategory}
            save={save}
          />
        </div>

        <div className="account-settings__row">
          <span className="account-field-label">
            {text.status}
            <span className="account-hint">{text.statusHint}</span>
          </span>
          <div>
            <SegmentedControl
              label={text.status}
              value={isOnBudget ? 'on' : 'off'}
              options={[
                { value: 'on', label: labels.onBudget },
                { value: 'off', label: labels.tracked }
              ]}
              onChange={(next) => {
                const nextOnBudget = next === 'on';
                setPending(
                  nextOnBudget === account.is_on_budget
                    ? null
                    : {
                        field: 'status',
                        payload: { is_on_budget: nextOnBudget },
                        effect: describeBudgetStatusChange(account, nextOnBudget)
                      }
                );
              }}
            />
            {renderConfirmation('status')}
          </div>
        </div>

        <div className="account-settings__row">
          <label className="account-field-label" htmlFor="account-setting-currency">
            {text.currency}
          </label>
          <div>
            <AccountSelect
              id="account-setting-currency"
              label={text.currency}
              value={currency}
              displayValue={currencyName ? `${currency} · ${currencyName}` : currency}
              groups={currencyGroups}
              searchPlaceholder={text.currencySearch}
              noResults={text.noResults}
              onChange={(next) =>
                setPending(
                  next === account.currency
                    ? null
                    : {
                        field: 'currency',
                        payload: { currency: next },
                        effect: describeCurrencyChange(account, next)
                      }
                )
              }
            />
            {renderConfirmation('currency')}
          </div>
        </div>

        <div className="account-settings__row">
          <label className="account-field-label" htmlFor="account-setting-kind">
            {text.kind}
          </label>
          <div>
            <AccountSelect
              id="account-setting-kind"
              label={text.kind}
              value={kind}
              displayValue={getAccountKindLabel(kind)}
              groups={kindGroups}
              onChange={(next) =>
                setPending(
                  next === account.account_kind
                    ? null
                    : { field: 'kind', payload: { account_kind: next }, effect: describeKindChange(account, next) }
                )
              }
            />
            {renderConfirmation('kind')}
          </div>
        </div>

        <div className="account-zone account-zone--archive">
          <div>
            <strong>{account.is_closed ? text.restoreTitle : text.archiveTitle}</strong>
            <p className="account-hint">{account.is_closed ? text.restoreHint : text.archiveHint}</p>
          </div>
          <button
            type="button"
            className="account-button"
            onClick={() =>
              void save(
                account,
                { is_closed: !account.is_closed },
                { message: account.is_closed ? text.restored : text.archived }
              )
            }
          >
            {account.is_closed ? labels.restore : labels.archive}
          </button>
        </div>
      </section>

      <section aria-label={text.dangerTitle}>
        <h2 className="account-danger-title">{text.dangerTitle}</h2>
        <div className={`account-zone account-zone--danger${deleteOpen ? ' account-zone--stacked' : ''}`}>
          <div>
            <strong>{text.deleteTitle}</strong>
            <p className="account-hint">{text.deleteHint}</p>
          </div>
          {deleteOpen ? (
            <>
              <div className="account-zone__confirm">
                <label className="account-field-label" htmlFor="account-delete-confirmation">
                  {text.deleteLabel(account.name)}
                </label>
                <input
                  id="account-delete-confirmation"
                  className="form-control"
                  autoComplete="off"
                  autoFocus
                  value={confirmation}
                  onChange={(event) => setConfirmation(event.target.value)}
                />
              </div>
              <div className="account-actions account-actions--start">
                <button
                  type="button"
                  className="account-button account-button--danger-solid"
                  disabled={confirmation.trim() !== account.name.trim() || deleting}
                  onClick={() => void handleDelete()}
                >
                  {text.deleteConfirm}
                </button>
                <button
                  type="button"
                  className="account-button"
                  disabled={deleting}
                  onClick={() => {
                    setDeleteOpen(false);
                    setConfirmation('');
                  }}
                >
                  {text.cancelChange}
                </button>
              </div>
            </>
          ) : (
            <button type="button" className="account-button account-button--danger" onClick={() => setDeleteOpen(true)}>
              {text.deleteOpen}
            </button>
          )}
        </div>
      </section>
    </div>
  );
};

export default AccountSettings;
