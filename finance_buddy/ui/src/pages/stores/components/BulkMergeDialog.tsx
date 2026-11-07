// „Scal wszystkie duplikaty”: lista scaleń z podglądem serwera dla każdego, jedno potwierdzenie.
// Scalenie z konfliktem blokującym jest pomijane. Każde da się cofnąć osobno na stronie sklepu.
import { useEffect, useState } from 'react';
import { toast } from 'sonner';
import { mergeStores, previewStoreMerge } from '../../../api';
import { copy } from '../../../copy/pl';
import { errorDetail } from '../apiError';
import { maskAccountNumbers, type MergePlan } from '../cleanup';
import StoreDialog from './StoreDialog';

const labels = copy.stores;
const text = labels.bulkMerge;
const CONCURRENCY = 3;

type PlanState =
  | { status: 'checking' }
  | { status: 'ready'; transactions: number }
  | { status: 'blocked'; message: string }
  | { status: 'done' }
  | { status: 'failed'; message: string };

type BulkMergeDialogProps = {
  plans: MergePlan[];
  onClose: () => void;
  onMerged: () => Promise<void>;
};

const inBatches = async <T,>(entries: readonly T[], run: (entry: T) => Promise<void>) => {
  for (let start = 0; start < entries.length; start += CONCURRENCY) {
    await Promise.all(entries.slice(start, start + CONCURRENCY).map(run));
  }
};

const BulkMergeDialog = ({ plans, onClose, onMerged }: BulkMergeDialogProps) => {
  const [states, setStates] = useState<ReadonlyMap<string, PlanState>>(
    () => new Map(plans.map((plan) => [plan.key, { status: 'checking' }]))
  );
  const [busy, setBusy] = useState(false);
  const setState = (key: string, state: PlanState) =>
    setStates((current) => new Map(current).set(key, state));

  // Okno dostaje plany raz, przy otwarciu, więc podgląd liczymy tylko wtedy
  useEffect(() => {
    let cancelled = false;
    void inBatches(plans, async (plan) => {
      try {
        const preview = await previewStoreMerge({ source_ids: plan.sourceIds, target_id: plan.targetId });
        if (cancelled) return;
        const blocking = preview.conflicts.find((conflict) => conflict.blocking);
        setState(
          plan.key,
          preview.can_merge
            ? { status: 'ready', transactions: preview.transactions }
            : { status: 'blocked', message: blocking?.message ?? text.blocked }
        );
      } catch (err) {
        if (!cancelled) setState(plan.key, { status: 'blocked', message: errorDetail(err) ?? text.blocked });
      }
    });
    return () => {
      cancelled = true;
    };
  }, [plans]);

  const ready = plans.filter((plan) => states.get(plan.key)?.status === 'ready');
  const checking = plans.some((plan) => states.get(plan.key)?.status === 'checking');
  const finished = plans.some((plan) => ['done', 'failed'].includes(states.get(plan.key)?.status ?? ''));

  const submit = async () => {
    setBusy(true);
    let merged = 0;
    let failed = 0;
    await inBatches(ready, async (plan) => {
      try {
        await mergeStores({
          source_ids: plan.sourceIds,
          target_id: plan.targetId,
          name: plan.name,
          category_id: plan.categoryId
        });
        merged += 1;
        setState(plan.key, { status: 'done' });
      } catch (err) {
        console.error(err);
        failed += 1;
        setState(plan.key, { status: 'failed', message: errorDetail(err) ?? text.failed });
      }
    });
    setBusy(false);
    if (failed) toast.error(text.partial(failed, merged + failed));
    else toast(text.merged(merged));
    await onMerged();
    if (!failed) onClose();
  };

  const describe = (plan: MergePlan) => {
    const state = states.get(plan.key);
    switch (state?.status) {
      case 'ready':
        return { text: labels.transactionsCount(state.transactions), blocking: false };
      case 'blocked':
      case 'failed':
        return { text: state.message, blocking: true };
      case 'done':
        return { text: text.done, blocking: false };
      default:
        return { text: text.checking, blocking: false };
    }
  };

  return (
    <StoreDialog
      title={text.title(plans.length)}
      wide
      onClose={busy ? () => undefined : onClose}
      footer={
        <>
          <button type="button" className="account-button" disabled={busy} onClick={onClose}>
            {finished ? labels.close : labels.cancel}
          </button>
          <button
            type="button"
            className="account-button account-button--primary"
            disabled={busy || checking || !ready.length}
            onClick={() => void submit()}
          >
            {busy ? text.merging : checking ? text.checking : text.confirm(ready.length)}
          </button>
        </>
      }
    >
      <p className="stores-dialog__note">{text.hint}</p>
      <ul className="stores-list stores-bulk-merge">
        {plans.map((plan) => {
          const status = describe(plan);
          const [target, ...others] = plan.groupNames.map(maskAccountNumbers);
          return (
            <li key={plan.key} className="stores-list__row">
              <span className="stores-list__name">
                {others.length ? text.into(others.join(', '), target) : text.sameNames(target, plan.bankNames)}
              </span>
              <span className={status.blocking ? 'stores-merge-conflict is-blocking' : 'stores-list__count'}>
                {status.text}
              </span>
            </li>
          );
        })}
      </ul>
    </StoreDialog>
  );
};

export default BulkMergeDialog;
