export interface Ingredient {
  name: string;
  qty: number;
  unit: string;
}

export interface Recipe {
  id: string;
  title: string;
  tags: string[];
  servings: number;
  prep_time: number;
  cook_time: number;
  kcal: number;
  p: number;
  f: number;
  c: number;
  hue: number;
  ingredients: Ingredient[];
  steps: string[];
  meal_types: string[];
  image_filename?: string | null;
}

export interface PlanEntry {
  day: number;
  meal: string;
  recipe_id: string;
  servings: number;
}

export interface MacroTarget {
  kcal: number;
  p: number;
  f: number;
  c: number;
}

export type PlanLayout = 'grid' | 'rows' | 'compact';
export type MacroViz = 'progress' | 'donut' | 'bar';

export interface Tweaks {
  planLayout: PlanLayout;
  macroViz: MacroViz;
  meals: string[];
}

export type SetTweak = <K extends keyof Tweaks>(key: K, value: Tweaks[K]) => void;

export interface WeekTemplate {
  id: number;
  name: string;
  entries: PlanEntry[];
  created_at: string;
}

export interface ShoppingItem {
  id: number;
  week_start: string;
  name: string;
  qty: number;
  unit: string;
  category: string;
  checked: boolean;
  is_custom: boolean;
}
