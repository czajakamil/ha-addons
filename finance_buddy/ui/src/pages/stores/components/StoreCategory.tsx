// Kategoria sklepu: kropka koloru i nazwa, pełna ścieżka w podpowiedzi.
import type { Category } from '../../../api';
import { TagColorDot } from '../../../components/tags/TagChip';
import { resolveTagColor } from '../../../components/tags/tagColors';
import TruncatedText from '../../../components/ui/TruncatedText';
import { formatCategoryPath } from '../../../utils/categoryPath';

type StoreCategoryProps = {
  categoryId?: string | null;
  categoryMap: Map<string, Category>;
  emptyLabel?: string;
};

const StoreCategory = ({ categoryId, categoryMap, emptyLabel = '—' }: StoreCategoryProps) => {
  const category = categoryId ? categoryMap.get(categoryId) : undefined;
  if (!category) return <span className="stores-muted">{emptyLabel}</span>;
  const { value, inherited } = resolveTagColor(category, categoryMap);
  return (
    <span className="stores-category" title={formatCategoryPath(category.path)}>
      <TagColorDot value={value} inherited={inherited} size={8} />
      <TruncatedText text={category.name} />
    </span>
  );
};

export default StoreCategory;
