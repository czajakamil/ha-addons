import type { LucideIcon } from 'lucide-react';
// Kuratorowany zestaw ikon i paleta kolorów dla awatara konta.
// Ikony trzymamy pod krótkim, stabilnym id (kebab-case) zapisywanym w bazie —
// dzięki temu nazwa komponentu Tabler może się zmienić bez migracji danych.
// Kolory to klucze palety motywu (szarość i tokeny --chart-*), więc awatar sam dostosowuje
// się do trybu jasnego/ciemnego.
import { BanknoteIcon, BitcoinIcon, BriefcaseBusinessIcon, BriefcaseIcon, CarIcon, ChartLineIcon, CircleDollarSignIcon, CoinsIcon, CreditCardIcon, DollarSignIcon, EuroIcon, GiftIcon, GlobeIcon, HandCoinsIcon, HeartIcon, HouseIcon, LandmarkIcon, PiggyBankIcon, PlaneIcon, ReceiptIcon, ShoppingCartIcon, SmartphoneIcon, StoreIcon, WalletIcon } from 'lucide-react';

// Wszystkie ikony Tabler mają identyczny typ komponentu — bierzemy go z jednej z nich.

export type AccountIconOption = { id: string; label: string; Icon: LucideIcon };

export const ACCOUNT_ICON_OPTIONS: AccountIconOption[] = [
  { id: 'wallet', label: 'Portfel', Icon: WalletIcon },
  { id: 'cash', label: 'Gotówka', Icon: BanknoteIcon },
  { id: 'bank', label: 'Bank', Icon: LandmarkIcon },
  { id: 'piggy', label: 'Skarbonka', Icon: PiggyBankIcon },
  { id: 'card', label: 'Karta', Icon: CreditCardIcon },
  { id: 'coins', label: 'Monety', Icon: CoinsIcon },
  { id: 'coin', label: 'Moneta', Icon: CircleDollarSignIcon },
  { id: 'moneybag', label: 'Worek', Icon: HandCoinsIcon },
  { id: 'dollar', label: 'Dolar', Icon: DollarSignIcon },
  { id: 'euro', label: 'Euro', Icon: EuroIcon },
  { id: 'bitcoin', label: 'Krypto', Icon: BitcoinIcon },
  { id: 'businessplan', label: 'Inwestycje', Icon: BriefcaseBusinessIcon },
  { id: 'chart', label: 'Wykres', Icon: ChartLineIcon },
  { id: 'briefcase', label: 'Firma', Icon: BriefcaseIcon },
  { id: 'home', label: 'Dom', Icon: HouseIcon },
  { id: 'car', label: 'Samochód', Icon: CarIcon },
  { id: 'plane', label: 'Podróże', Icon: PlaneIcon },
  { id: 'cart', label: 'Zakupy', Icon: ShoppingCartIcon },
  { id: 'store', label: 'Sklep', Icon: StoreIcon },
  { id: 'receipt', label: 'Rachunki', Icon: ReceiptIcon },
  { id: 'gift', label: 'Prezenty', Icon: GiftIcon },
  { id: 'heart', label: 'Zdrowie', Icon: HeartIcon },
  { id: 'world', label: 'Świat', Icon: GlobeIcon },
  { id: 'phone', label: 'Telefon', Icon: SmartphoneIcon }
];

const ACCOUNT_ICON_MAP = new Map(ACCOUNT_ICON_OPTIONS.map((option) => [option.id, option]));

export const DEFAULT_ACCOUNT_ICON = 'wallet';

export const getAccountIcon = (id?: string | null): LucideIcon =>
  (id ? ACCOUNT_ICON_MAP.get(id)?.Icon : undefined) ??
  ACCOUNT_ICON_MAP.get(DEFAULT_ACCOUNT_ICON)!.Icon;

export type AccountColorOption = { id: string; label: string; value: string };

export const ACCOUNT_COLOR_OPTIONS: AccountColorOption[] = [
  // Id „indigo” zostaje dla szarości, bo jest zapisane w bazie jako domyślny kolor kont.
  { id: 'indigo', label: 'Szary', value: 'var(--text-muted)' },
  { id: 'blue', label: 'Niebieski', value: 'var(--chart-1)' },
  { id: 'teal', label: 'Morski', value: 'var(--chart-3)' },
  { id: 'amber', label: 'Bursztynowy', value: 'var(--chart-4)' },
  { id: 'pink', label: 'Różowy', value: 'var(--chart-5)' },
  { id: 'green', label: 'Zielony', value: 'var(--chart-6)' },
  { id: 'violet', label: 'Fioletowy', value: 'var(--chart-7)' },
  { id: 'coral', label: 'Koralowy', value: 'var(--chart-8)' }
];

const ACCOUNT_COLOR_MAP = new Map(ACCOUNT_COLOR_OPTIONS.map((option) => [option.id, option]));

export const DEFAULT_ACCOUNT_COLOR = 'indigo';

export const getAccountColorValue = (id?: string | null): string =>
  (id ? ACCOUNT_COLOR_MAP.get(id)?.value : undefined) ??
  ACCOUNT_COLOR_MAP.get(DEFAULT_ACCOUNT_COLOR)!.value;
