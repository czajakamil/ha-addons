// Reguły widoku „Do uporządkowania”. Każdy sklep ma jeden powód, żeby liczniki chipów sumowały się do całości.
import type { Store } from '../../api';

export type CleanupReason = 'self' | 'duplicate' | 'raw' | 'uncategorized';

export type CleanupItem = {
  store: Store;
  reason: CleanupReason;
  /** Inny sklep o tej samej nazwie (dla duplikatu). */
  duplicateOf?: Store;
  /** Czytelna nazwa wycięta z surowej (dla surowej nazwy z banku). */
  proposedName?: string;
  rawKind?: 'digits' | 'address' | 'account';
};

const LEGAL_FORMS = [/\bsp\.?\s*z\s*o\.?\s*o\.?/gi, /\bs\.\s*a\.?(?=\s|$)/gi, /\bsa$/gi];
const NAME_CUT_RE = /\s(?:n\.?v\.?|b\.?v\.?|gmbh|ltd\.?|inc\.?|sp\.?\s*z\s*o|s\.\s*a\.?|ul\.|al\.)(?:\s|$)|[,\d]/i;

const SPACED_ACCOUNT = String.raw`[A-Z]{2}\d{2}(?: ?\d{4}){2,}(?: ?\d{1,4})?`;
const JOINED_ACCOUNT = String.raw`[A-Z]{2}\d{2}[A-Z0-9]{10,30}`;
const DOMESTIC_ACCOUNT = String.raw`\d{2}(?: ?\d{4}){5,6}`;
const ACCOUNT_RE = new RegExp(String.raw`\b(?:${SPACED_ACCOUNT}|${JOINED_ACCOUNT}|${DOMESTIC_ACCOUNT})\b`, 'g');

const POSTAL_CODE_RE = /\b\d{2}-\d{3}\b|\b\d{4} ?[A-Z]{2}\b/;
const ADDRESS_RE = /\b(?:ul\.|al\.|pl\.|ulica|aleja|road|avenue)|(?:straat|strasse|street)\b/i;
const DIGITS_ONLY_RE = /^[\d\s]+$/;
const SELF_MARKER_RE = /sent from|account:/i;

/** `LT13 3250 0000 0000 1248` → `LT13 •••• 1248`. Pokazujemy to wszędzie, gdzie nazwa trafia do UI. */
export const maskAccountNumbers = (text: string) =>
  text.replace(ACCOUNT_RE, (match) => {
    const compact = match.replace(/ /g, '');
    return `${compact.slice(0, 4)} •••• ${compact.slice(-4)}`;
  });

export const normalizeStoreName = (name: string) => {
  const stripped = LEGAL_FORMS.reduce((text, form) => text.replace(form, ' '), name)
    .toLocaleLowerCase('pl-PL')
    .replace(/[.,]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return stripped || name.trim().toLocaleLowerCase('pl-PL');
};

const hasOwnerName = (name: string, ownerName: string) => {
  const haystack = name.toLocaleLowerCase('pl-PL');
  const parts = ownerName.toLocaleLowerCase('pl-PL').split(/\s+/).filter(Boolean);
  return parts.length > 0 && parts.every((part) => haystack.includes(part));
};

const proposeName = (raw: string) => {
  const name = raw.replace(ACCOUNT_RE, ' ').trim();
  const cut = name.search(NAME_CUT_RE);
  const head = (cut > 0 ? name.slice(0, cut) : name.split(/\s+/).slice(0, 2).join(' ')).trim();
  if (head.length < 2) return undefined;
  return head === head.toLocaleUpperCase('pl-PL')
    ? head.charAt(0) + head.slice(1).toLocaleLowerCase('pl-PL')
    : head;
};

const rawKindOf = (name: string): CleanupItem['rawKind'] => {
  if (DIGITS_ONLY_RE.test(name.trim())) return 'digits';
  if (name.search(ACCOUNT_RE) >= 0) return 'account';
  if (POSTAL_CODE_RE.test(name) || ADDRESS_RE.test(name)) return 'address';
  return undefined;
};

export const buildCleanupItems = (stores: readonly Store[], ownerName = ''): CleanupItem[] => {
  const active = stores.filter((store) => !store.is_deleted);
  const byNormalizedName = new Map<string, Store[]>();
  active.forEach((store) => {
    const key = normalizeStoreName(store.name);
    byNormalizedName.set(key, [...(byNormalizedName.get(key) ?? []), store]);
  });

  return active.flatMap((store): CleanupItem[] => {
    if (ownerName && hasOwnerName(store.name, ownerName) && SELF_MARKER_RE.test(store.name)) {
      return [{ store, reason: 'self' }];
    }
    const namesakes = byNormalizedName.get(normalizeStoreName(store.name)) ?? [];
    if (namesakes.length > 1) {
      return [{ store, reason: 'duplicate', duplicateOf: namesakes.find((other) => other.id !== store.id) }];
    }
    const rawKind = rawKindOf(store.name);
    if (rawKind) {
      return [{ store, reason: 'raw', rawKind, proposedName: rawKind === 'digits' ? undefined : proposeName(store.name) }];
    }
    return store.automatic_category_id ? [] : [{ store, reason: 'uncategorized' }];
  });
};
