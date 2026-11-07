// Karta na stronie sklepu: „Scal z…” z wyborem sklepu i lista scaleń, które można jeszcze cofnąć.
import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { toast } from 'sonner';
import { type Category, getStoreMerges, type StoreMerge, undoStoreMerge } from '../../../api';
import { copy } from '../../../copy/pl';
import { useApi } from '../../../hooks/useApi';
import { formatShortDate } from '../../../utils/date';
import AccountSelect from '../../accounts/components/AccountSelect';
import { errorDetail } from '../apiError';
import { maskAccountNumbers } from '../cleanup';
import type { StoreGroup } from '../groups';
import MergeDialog from './MergeDialog';

const labels = copy.stores;
const text = labels.settings;
const dialogText = labels.mergeDialog;

type StoreMergeCardProps = {
  group: StoreGroup;
  otherGroups: StoreGroup[];
  categories: Category[];
  categoryMap: Map<string, Category>;
  ensureCategory: (path: string) => Promise<string>;
  onChanged: () => Promise<void>;
};

const StoreMergeCard = ({ group, otherGroups, categories, categoryMap, ensureCategory, onChanged }: StoreMergeCardProps) => {
  const navigate = useNavigate();
  const ids = group.members.map((store) => store.id);
  const merges = useApi(() => getStoreMerges(ids));
  const [picking, setPicking] = useState(false);
  const [targetKey, setTargetKey] = useState<string | null>(null);
  const [mergingMembers, setMergingMembers] = useState(false);
  const target = otherGroups.find((entry) => entry.key === targetKey);
  const categoryId = target?.categoryId ?? group.categoryId;

  const undo = async (merge: StoreMerge) => {
    try {
      await undoStoreMerge(merge.id);
      toast(dialogText.undone);
      await Promise.all([merges.refetch(), onChanged()]);
    } catch (err) {
      console.error(err);
      toast.error(errorDetail(err) ?? dialogText.undoFailed);
    }
  };

  return (
    <section className="stores-merge-card" aria-label={text.mergeTitle}>
      {group.members.length > 1 && (
        <div className="stores-merge-card__row">
          <div className="stores-merge-card__copy">
            <strong>{text.membersTitle}</strong>
            <p className="account-hint">{text.membersHint(group.members.length)}</p>
          </div>
          <button type="button" className="account-button" onClick={() => setMergingMembers(true)}>
            {text.membersButton}
          </button>
        </div>
      )}
      <div className="stores-merge-card__row">
        <div className="stores-merge-card__copy">
          <strong>{text.mergeTitle}</strong>
          <p className="account-hint">{text.mergeHint}</p>
        </div>
        {picking ? (
          <div className="stores-merge-card__picker">
            <AccountSelect
              id="store-merge-target"
              label={text.mergePick}
              value=""
              displayValue={text.mergePick}
              groups={[
                otherGroups.map((entry) => ({
                  value: entry.key,
                  label: maskAccountNumbers(entry.name),
                  keywords: [entry.name, ...entry.members.map((store) => store.name)]
                }))
              ]}
              searchPlaceholder={text.mergeSearch}
              noResults={text.mergeNoResults}
              onChange={setTargetKey}
            />
          </div>
        ) : (
          <button type="button" className="account-button" disabled={!otherGroups.length} onClick={() => setPicking(true)}>
            {text.mergeButton}
          </button>
        )}
      </div>
      {Boolean(merges.data?.length) && (
        <div className="stores-dialog__field">
          <span className="account-field-label">{text.mergesTitle}</span>
          <ul className="stores-list">
            {merges.data?.map((merge) => (
              <li key={merge.id} className="stores-list__row">
                <span className="stores-list__name">
                  {text.mergeEntry(
                    merge.source_names.map(maskAccountNumbers).join(', '),
                    merge.transactions,
                    formatShortDate(merge.expires_at.slice(0, 10))
                  )}
                </span>
                <button type="button" className="stores-aliases__remove" onClick={() => void undo(merge)}>
                  {labels.undo}
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}
      {mergingMembers && (
        <MergeDialog
          sourceIds={ids.filter((id) => id !== group.id)}
          targetId={group.id}
          defaultName={group.name}
          names={group.members.map((store) => store.name)}
          categories={categories}
          categoryPath={(group.categoryId && categoryMap.get(group.categoryId)?.path) || ''}
          ensureCategory={ensureCategory}
          onClose={() => setMergingMembers(false)}
          onMerged={async () => {
            setMergingMembers(false);
            await Promise.all([merges.refetch(), onChanged()]);
          }}
          onUndone={onChanged}
        />
      )}
      {target && (
        <MergeDialog
          sourceIds={ids}
          targetId={target.id}
          defaultName={target.name}
          names={[target.name, ...group.members.map((store) => store.name)]}
          categories={categories}
          categoryPath={(categoryId && categoryMap.get(categoryId)?.path) || ''}
          ensureCategory={ensureCategory}
          onClose={() => setTargetKey(null)}
          onMerged={async (targetId) => {
            setTargetKey(null);
            setPicking(false);
            navigate(`/payees/${targetId}?tab=settings`);
            await onChanged();
          }}
          onUndone={onChanged}
        />
      )}
    </section>
  );
};

export default StoreMergeCard;
