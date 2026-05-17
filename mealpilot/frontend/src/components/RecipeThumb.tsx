import { recipeImageUrl } from '../data';
import type { Recipe } from '../types';

interface Props {
  recipe: Pick<Recipe, 'id' | 'hue' | 'image_filename'> | undefined | null;
  h?: number;
}

export function RecipeThumb({ recipe, h = 80 }: Props) {
  const hue = recipe?.hue ?? 40;
  const src = recipe ? recipeImageUrl(recipe) : null;

  if (src) {
    return (
      <div
        style={{
          height: h,
          borderRadius: 'var(--r)',
          border: '1px solid var(--line-soft)',
          overflow: 'hidden',
          flexShrink: 0,
        }}
      >
        <img
          src={src}
          alt=""
          style={{ width: '100%', height: '100%', objectFit: 'cover', display: 'block' }}
        />
      </div>
    );
  }

  return (
    <div
      style={{
        height: h,
        borderRadius: 'var(--r)',
        background: `repeating-linear-gradient(135deg, oklch(0.93 0.04 ${hue}) 0 10px, oklch(0.96 0.03 ${hue}) 10px 20px)`,
        border: '1px solid var(--line-soft)',
        position: 'relative',
        overflow: 'hidden',
        flexShrink: 0,
      }}
    >
      <div
        style={{
          position: 'absolute',
          inset: 0,
          background: `linear-gradient(180deg, transparent 50%, oklch(0.55 0.13 ${hue} / 0.18))`,
        }}
      />
      <div
        style={{
          position: 'absolute',
          bottom: 6,
          left: 8,
          fontFamily: 'var(--mono)',
          fontSize: 9,
          letterSpacing: '.08em',
          textTransform: 'uppercase',
          color: `oklch(0.32 0.10 ${hue})`,
          opacity: 0.7,
        }}
      >
        zdjęcie
      </div>
    </div>
  );
}
