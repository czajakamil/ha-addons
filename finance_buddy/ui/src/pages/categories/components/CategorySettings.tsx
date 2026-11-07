// Zakładka Ustawienia strony kategorii: rodzic, kolor i usuwanie (tylko pustej kategorii).
import { useState } from 'react';
import type { Category, CategoryUpdate } from '../../../api';
import TagColorPicker from '../../../components/tags/TagColorPicker';
import { resolveTagColor } from '../../../components/tags/tagColors';
import { copy } from '../../../copy/pl';
import ParentSelect from './ParentSelect';

const text = copy.categories.details;

type CategorySettingsProps = {
  category: Category;
  categoryMap: Map<string, Category>;
  /** null, dopóki statystyki się wczytują. */
  transactionCount: number | null;
  parentTargets: Category[];
  onSave: (payload: CategoryUpdate) => Promise<void>;
  onDelete: () => void;
};

const CategorySettings = ({
  category,
  categoryMap,
  transactionCount,
  parentTargets,
  onSave,
  onDelete
}: CategorySettingsProps) => {
  const [saving, setSaving] = useState(false);
  const canDelete = transactionCount === 0;

  const save = async (payload: CategoryUpdate) => {
    setSaving(true);
    await onSave(payload);
    setSaving(false);
  };

  return (
    <div className="account-settings">
      <section className="account-settings__card" aria-label={copy.categories.details.tabs.settings}>
        <div className="account-settings__row">
          <label className="account-field-label" htmlFor="category-setting-parent">
            {text.parent}
            <span className="account-hint">{text.parentHint}</span>
          </label>
          <ParentSelect
            id="category-setting-parent"
            value={category.parent_id ?? null}
            targets={parentTargets}
            disabled={saving}
            onChange={(parentId) => {
              if (parentId !== (category.parent_id ?? null)) void save({ parent_id: parentId });
            }}
          />
        </div>
        <div className="account-settings__row">
          <span className="account-field-label">
            {text.color}
            <span className="account-hint">{text.colorHint}</span>
          </span>
          <div>
            <TagColorPicker
              resolved={resolveTagColor(category, categoryMap)}
              ownColorId={category.color ?? null}
              canInherit={!!category.parent_id}
              disabled={saving}
              onSelect={(colorId) => {
                if (colorId !== (category.color ?? null)) void save({ color: colorId });
              }}
            />
          </div>
        </div>
      </section>

      <section aria-label={text.dangerTitle}>
        <h2 className="account-danger-title">{text.dangerTitle}</h2>
        <div className="account-zone account-zone--danger">
          <div>
            <strong>{text.deleteTitle}</strong>
            <p className="account-hint">
              {transactionCount === null
                ? '…'
                : canDelete
                  ? text.deleteEmptyHint
                  : text.deleteBlockedHint(transactionCount)}
            </p>
          </div>
          <button
            type="button"
            className="account-button account-button--danger"
            disabled={!canDelete}
            onClick={onDelete}
          >
            {copy.categories.menu.delete}
          </button>
        </div>
      </section>
    </div>
  );
};

export default CategorySettings;
