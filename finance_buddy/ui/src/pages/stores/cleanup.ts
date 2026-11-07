// Reguły widoku „Do uporządkowania”. Pozycją jest grupa sklepów, tak jak w widoku „Wszystkie”,
// a każda grupa ma jeden powód, żeby liczniki chipów sumowały się do całości.
import type { Store } from '../../api';
import type { StoreGroup } from './groups';

export type CleanupReason = 'self' | 'duplicate' | 'raw' | 'uncategorized';

export type CleanupItem = {
  /** Główny sklep grupy: jego id otwiera stronę sklepu i jest kluczem zaznaczenia. */
  store: Store;
  group: StoreGroup;
  reason: CleanupReason;
  /** Inna grupa o tej samej nazwie (dla duplikatu). */
  duplicateOf?: StoreGroup;
  /** Alias tej nazwy jest niejednoznaczny, więc importer go nie używa. */
  isAmbiguous?: boolean;
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

const words = (text: string) => text.toLocaleLowerCase('pl-PL').split(/[^\p{L}\p{N}]+/u).filter(Boolean);

/**
 * Imię i nazwisko zalogowanego użytkownika, jako całe słowa. Samo imię (albo login) nie wystarcza:
 * „Kamil” pasowałby do przelewu od każdego innego Kamila.
 */
const hasOwnerName = (name: string, ownerName: string) => {
  const parts = words(ownerName);
  if (parts.length < 2) return false;
  const haystack = new Set(words(name));
  return parts.every((part) => haystack.has(part));
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

const isSelfTransfer = (name: string, ownerName: string) =>
  Boolean(ownerName) && hasOwnerName(name, ownerName) && SELF_MARKER_RE.test(name);

/**
 * Surowa nazwa liczy się tylko jako nazwa grupy: po zmianie nazwy grupa jest uporządkowana,
 * choć nazwy z banku zostają (importer po nich dopasowuje). Kategoria jednego sklepu wystarcza całej grupie.
 */
export const buildCleanupItems = (
  groups: readonly StoreGroup[],
  ownerName = '',
  ambiguousStoreIds: ReadonlySet<string> = new Set()
): CleanupItem[] => {
  const active = groups.filter((group) => !group.isArchived);
  const byNormalizedName = new Map<string, StoreGroup[]>();
  active.forEach((group) => {
    const key = normalizeStoreName(group.name);
    byNormalizedName.set(key, [...(byNormalizedName.get(key) ?? []), group]);
  });

  return active.flatMap((group): CleanupItem[] => {
    const store = group.members.find((member) => member.id === group.id) ?? group.members[0];
    if (group.members.some((member) => isSelfTransfer(member.name, ownerName))) {
      return [{ store, group, reason: 'self' }];
    }
    const namesakes = byNormalizedName.get(normalizeStoreName(group.name)) ?? [];
    const isAmbiguous = group.members.some((member) => ambiguousStoreIds.has(member.id));
    if (namesakes.length > 1 || isAmbiguous) {
      const duplicateOf = namesakes.find((other) => other.key !== group.key);
      return [{ store, group, reason: 'duplicate', isAmbiguous, duplicateOf }];
    }
    const rawKind = rawKindOf(group.name);
    if (rawKind) {
      const proposedName = rawKind === 'digits' ? undefined : proposeName(group.name);
      return [{ store, group, reason: 'raw', rawKind, proposedName }];
    }
    return group.categoryId ? [] : [{ store, group, reason: 'uncategorized' }];
  });
};
