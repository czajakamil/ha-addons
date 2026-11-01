// Zakładka Ustawienia strony sklepu. Pola zapisują się same, z „Cofnij” w toaście.
import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { toast } from 'sonner';
import { type Category, type Store, type StoreUpdate, deleteStore } from '../../../api';
import TagPathInput from '../../../components/tags/TagPathInput';
import { copy } from '../../../copy/pl';
import { isValidNip } from '../../../utils/nip';
import { maskAccountNumbers } from '../cleanup';
import type { StoreGroup } from '../groups';
import { categoryPayload } from '../useStoreGroupSave';
import StoreAliases from './StoreAliases';
import StoreMergeCard from './StoreMergeCard';
import StoreTransfersCard from './StoreTransfersCard';

const labels = copy.stores;
const text = labels.settings;

type Save = (changes: { store: Store; payload: StoreUpdate }[], message?: string) => Promise<boolean>;

type StoreSettingsProps = {
  group: StoreGroup;
  // null: liczba transakcji nieznana, więc usuwanie zostaje zablokowane
  transactionCount: number | null;
  categories: Category[];
  categoryPath: string;
  ensureCategory: (path: string) => Promise<string>;
  save: Save;
  otherGroups: StoreGroup[];
  categoryMap: Map<string, Category>;
  onChanged: () => Promise<void>;
};

const CategoryField = ({ group, categories, categoryPath, ensureCategory, save }: Pick<StoreSettingsProps, 'group' | 'categories' | 'categoryPath' | 'ensureCategory' | 'save'>) => {
  const [value, setValue] = useState(categoryPath);

  const commit = async (path: string) => {
    const next = path.trim();
    if (next === categoryPath) return;
    try {
      const categoryId = next ? await ensureCategory(next) : null;
      await save(group.members.map((store) => ({ store, payload: categoryPayload(store, categoryId) })));
    } catch (err) {
      console.error(err);
      toast.error(labels.categoryFailed);
      setValue(categoryPath);
    }
  };

  return (
    <TagPathInput
      inputId="store-setting-category"
      value={value}
      categories={categories}
      forAccounts={false}
      placeholder={text.categoryPlaceholder}
      onChange={setValue}
      onCommit={(path) => void commit(path)}
      onCancel={() => setValue(categoryPath)}
    />
  );
};

const TextField = ({
  id,
  value,
  placeholder,
  validate,
  onSave
}: {
  id: string;
  value: string;
  placeholder: string;
  validate?: (next: string) => string | null;
  onSave: (next: string | null) => Promise<boolean>;
}) => {
  const [draft, setDraft] = useState(value);

  const commit = async () => {
    const next = draft.trim();
    if (next === value) return;
    const problem = next ? validate?.(next) : null;
    if (problem) {
      toast.error(problem);
      setDraft(value);
      return;
    }
    if (!(await onSave(next || null))) setDraft(value);
  };

  return (
    <input
      id={id}
      className="form-control"
      autoComplete="off"
      value={draft}
      placeholder={placeholder}
      onChange={(event) => setDraft(event.target.value)}
      onBlur={() => void commit()}
      onKeyDown={(event) => {
        if (event.key === 'Enter') event.currentTarget.blur();
        if (event.key === 'Escape') setDraft(value);
      }}
    />
  );
};

const StoreSettings = ({
  group,
  transactionCount,
  categories,
  categoryPath,
  ensureCategory,
  save,
  otherGroups,
  categoryMap,
  onChanged
}: StoreSettingsProps) => {
  const navigate = useNavigate();
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [confirmation, setConfirmation] = useState('');
  const [deleting, setDeleting] = useState(false);
  // NIP i adres są na jednym sklepie grupy; gdy któryś już je ma, zostają tam
  const nipStore = group.members.find((store) => store.nip) ?? group.members[0];
  const addressStore = group.members.find((store) => store.address) ?? group.members[0];
  const displayName = maskAccountNumbers(group.name);
  const canDelete = transactionCount === 0;

  const handleDelete = async () => {
    try {
      setDeleting(true);
      await Promise.all(group.members.map((store) => deleteStore(store.id)));
      toast(text.deleted);
      navigate('/stores');
    } catch (err) {
      console.error(err);
      toast.error(text.deleteFailed);
      setDeleting(false);
    }
  };

  const deleteHint =
    transactionCount === null
      ? text.deleteUnknown
      : canDelete
        ? text.deleteAvailable
        : text.deleteBlocked(transactionCount);

  return (
    <div className="account-settings">
      <section className="account-settings__card" aria-label={labels.tabs.settings}>
        <div className="account-settings__row">
          <label className="account-field-label" htmlFor="store-setting-category">
            {labels.columns.category}
            <span className="account-hint">{text.categoryHint}</span>
          </label>
          <CategoryField
            key={categoryPath}
            group={group}
            categories={categories}
            categoryPath={categoryPath}
            ensureCategory={ensureCategory}
            save={save}
          />
        </div>

        <div className="account-settings__row">
          <span className="account-field-label">
            {text.aliases}
            <span className="account-hint">{text.aliasesHint}</span>
          </span>
          <StoreAliases storeIds={group.members.map((store) => store.id)} targetStoreId={group.id} />
        </div>

        <div className="account-settings__row">
          <label className="account-field-label" htmlFor="store-setting-nip">
            {text.nip} <span className="stores-muted">{text.optional}</span>
          </label>
          <TextField
            key={`nip-${nipStore.id}-${nipStore.nip ?? ''}`}
            id="store-setting-nip"
            value={nipStore.nip ?? ''}
            placeholder={text.nipPlaceholder}
            validate={(next) => (isValidNip(next) ? null : text.nipInvalid)}
            onSave={(nip) => save([{ store: nipStore, payload: { nip } }])}
          />
        </div>

        <div className="account-settings__row">
          <label className="account-field-label" htmlFor="store-setting-address">
            {text.address} <span className="stores-muted">{text.optional}</span>
          </label>
          <TextField
            key={`address-${addressStore.id}-${addressStore.address ?? ''}`}
            id="store-setting-address"
            value={addressStore.address ?? ''}
            placeholder={text.addressPlaceholder}
            onSave={(address) => save([{ store: addressStore, payload: { address } }])}
          />
        </div>
      </section>

      <StoreTransfersCard storeIds={group.members.map((store) => store.id)} onChanged={onChanged} />

      <StoreMergeCard
        group={group}
        otherGroups={otherGroups}
        categories={categories}
        categoryMap={categoryMap}
        ensureCategory={ensureCategory}
        onChanged={onChanged}
      />

      <div className="account-zone stores-zone">
        <div>
          <strong>{group.isArchived ? text.restoreTitle : text.archiveTitle}</strong>
          <p className="account-hint">{group.isArchived ? text.restoreHint : text.archiveHint}</p>
        </div>
        <button
          type="button"
          className="account-button"
          onClick={() =>
            void save(
              group.members.map((store) => ({ store, payload: { is_deleted: !group.isArchived } })),
              group.isArchived ? labels.restored : labels.archived
            )
          }
        >
          {group.isArchived ? labels.restore : labels.archive}
        </button>
      </div>

      <section aria-label={text.dangerTitle}>
        <h2 className="account-danger-title">{text.dangerTitle}</h2>
        <div className={`account-zone account-zone--danger${deleteOpen ? ' account-zone--stacked' : ''}`}>
          <div>
            <strong>{text.deleteTitle}</strong>
            <p className="account-hint">{deleteHint}</p>
          </div>
          {deleteOpen && canDelete ? (
            <>
              <div className="account-zone__confirm">
                <label className="account-field-label" htmlFor="store-delete-confirmation">
                  {text.deleteLabel(displayName)}
                </label>
                <input
                  id="store-delete-confirmation"
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
                  disabled={confirmation.trim() !== displayName.trim() || deleting}
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
                  {labels.cancel}
                </button>
              </div>
            </>
          ) : (
            <button
              type="button"
              className="account-button account-button--danger"
              disabled={!canDelete}
              onClick={() => setDeleteOpen(true)}
            >
              {text.deleteOpen}
            </button>
          )}
        </div>
      </section>
    </div>
  );
};

export default StoreSettings;
