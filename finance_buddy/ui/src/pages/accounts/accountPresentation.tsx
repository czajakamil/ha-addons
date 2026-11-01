// Kuratorowany zestaw ikon i paleta kolorów dla awatara konta.
// Ikony trzymamy pod krótkim, stabilnym id (kebab-case) zapisywanym w bazie —
// dzięki temu nazwa komponentu Tabler może się zmienić bez migracji danych.
// Kolory to klucze palety motywu (tokeny --cat-*), więc awatar sam dostosowuje
// się do trybu jasnego/ciemnego.
import {
  IconWallet,
  IconCash,
  IconBuildingBank,
  IconPigMoney,
  IconCreditCard,
  IconCoins,
  IconCoin,
  IconCurrencyDollar,
  IconCurrencyEuro,
  IconCurrencyBitcoin,
  IconBusinessplan,
  IconChartLine,
  IconBriefcase,
  IconHome,
  IconCar,
  IconPlane,
  IconShoppingCart,
  IconGift,
  IconHeart,
  IconWorld,
  IconDeviceMobile,
  IconBuildingStore,
  IconReceipt,
  IconMoneybag
} from '@tabler/icons-react';

// Wszystkie ikony Tabler mają identyczny typ komponentu — bierzemy go z jednej z nich.
export type TablerIcon = typeof IconWallet;

export type AccountIconOption = { id: string; label: string; Icon: TablerIcon };

export const ACCOUNT_ICON_OPTIONS: AccountIconOption[] = [
  { id: 'wallet', label: 'Portfel', Icon: IconWallet },
  { id: 'cash', label: 'Gotówka', Icon: IconCash },
  { id: 'bank', label: 'Bank', Icon: IconBuildingBank },
  { id: 'piggy', label: 'Skarbonka', Icon: IconPigMoney },
  { id: 'card', label: 'Karta', Icon: IconCreditCard },
  { id: 'coins', label: 'Monety', Icon: IconCoins },
  { id: 'coin', label: 'Moneta', Icon: IconCoin },
  { id: 'moneybag', label: 'Worek', Icon: IconMoneybag },
  { id: 'dollar', label: 'Dolar', Icon: IconCurrencyDollar },
  { id: 'euro', label: 'Euro', Icon: IconCurrencyEuro },
  { id: 'bitcoin', label: 'Krypto', Icon: IconCurrencyBitcoin },
  { id: 'businessplan', label: 'Inwestycje', Icon: IconBusinessplan },
  { id: 'chart', label: 'Wykres', Icon: IconChartLine },
  { id: 'briefcase', label: 'Firma', Icon: IconBriefcase },
  { id: 'home', label: 'Dom', Icon: IconHome },
  { id: 'car', label: 'Samochód', Icon: IconCar },
  { id: 'plane', label: 'Podróże', Icon: IconPlane },
  { id: 'cart', label: 'Zakupy', Icon: IconShoppingCart },
  { id: 'store', label: 'Sklep', Icon: IconBuildingStore },
  { id: 'receipt', label: 'Rachunki', Icon: IconReceipt },
  { id: 'gift', label: 'Prezenty', Icon: IconGift },
  { id: 'heart', label: 'Zdrowie', Icon: IconHeart },
  { id: 'world', label: 'Świat', Icon: IconWorld },
  { id: 'phone', label: 'Telefon', Icon: IconDeviceMobile }
];

const ACCOUNT_ICON_MAP = new Map(ACCOUNT_ICON_OPTIONS.map((option) => [option.id, option]));

export const DEFAULT_ACCOUNT_ICON = 'wallet';

export const getAccountIcon = (id?: string | null): TablerIcon =>
  (id ? ACCOUNT_ICON_MAP.get(id)?.Icon : undefined) ??
  ACCOUNT_ICON_MAP.get(DEFAULT_ACCOUNT_ICON)!.Icon;

export type AccountColorOption = { id: string; label: string; value: string };

export const ACCOUNT_COLOR_OPTIONS: AccountColorOption[] = [
  { id: 'indigo', label: 'Indygo', value: 'var(--cat-indigo)' },
  { id: 'teal', label: 'Morski', value: 'var(--cat-teal)' },
  { id: 'green', label: 'Zielony', value: 'var(--cat-green)' },
  { id: 'coral', label: 'Koralowy', value: 'var(--cat-coral)' },
  { id: 'amber', label: 'Bursztynowy', value: 'var(--cat-amber)' },
  { id: 'pink', label: 'Różowy', value: 'var(--cat-pink)' },
  { id: 'blue', label: 'Niebieski', value: 'var(--cat-blue)' },
  { id: 'violet', label: 'Fioletowy', value: 'var(--cat-violet)' }
];

const ACCOUNT_COLOR_MAP = new Map(ACCOUNT_COLOR_OPTIONS.map((option) => [option.id, option]));

export const DEFAULT_ACCOUNT_COLOR = 'indigo';

export const getAccountColorValue = (id?: string | null): string =>
  (id ? ACCOUNT_COLOR_MAP.get(id)?.value : undefined) ??
  ACCOUNT_COLOR_MAP.get(DEFAULT_ACCOUNT_COLOR)!.value;
