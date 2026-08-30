export interface Ingredient {
  name: string;
  qty: number;
  unit: string;
}

export interface Step {
  text: string;
  duration_minutes?: number | null;
}

export interface Recipe {
  id: number;
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
  steps: Step[];
  meal_types: string[];
  image_filename?: string | null;
  is_meal_prep?: boolean;
  meal_prep_days?: number | null;
  meal_prep_steps?: Step[];
  created_by?: number;
  owner_user_id?: number | null;
  owner_household_id?: number | null;
  avg_rating?: number | null;
  rating_count?: number;
  my_rating?: number | null;
  my_note?: string | null;
}

export interface PlanEntry {
  day: number;
  meal: string;
  recipe_id: number;
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
  recipe_ids: number[];
}

export interface Household {
  id: number;
  name: string;
  created_at: string;
  member_count: number;
}

export interface HouseholdMember {
  user_id: number;
  username: string;
  household_id: number;
  can_edit: boolean;
  joined_at: string;
}

export interface AdminUser {
  id: number;
  username: string;
  role: 'admin' | 'user';
  is_active: boolean;
  created_at: string;
  can_use_ai: boolean;
  ai_monthly_token_limit: number | null;
  ai_monthly_cost_limit_cents: number | null;
  ai_used_tokens_this_month: number;
  ai_used_cost_cents_this_month: number;
  household_id: number | null;
  can_edit_in_household: boolean;
}

export interface AiUsageStatus {
  can_use_ai: boolean;
  ai_monthly_token_limit: number | null;
  ai_monthly_cost_limit_cents: number | null;
  ai_used_tokens_this_month: number;
  ai_used_cost_cents_this_month: number;
}
