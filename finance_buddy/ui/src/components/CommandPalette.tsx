// Globalne skróty: Cmd/Ctrl+K otwiera paletę, N otwiera dodawanie wydatku z dowolnego ekranu.
import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Command } from 'cmdk';
import { Plus, Wallet, PiggyBank } from 'lucide-react';
import { toast } from 'sonner';
import {
  getAccounts,
  getAllCategories,
  getAllStores,
  type Account,
  type Category,
  type Store
} from '../api';
import { copy } from '../copy/pl';
import { refreshUntaggedCount } from '../hooks/useUntaggedCount';
import { navGroups } from './AppSidebar';
import NewTransactionModal from './transactions/NewTransactionModal';

const labels = copy.palette;

type Lookups = { accounts: Account[]; stores: Store[]; categories: Category[] };

const isTypingTarget = (target: EventTarget | null) =>
  target instanceof HTMLElement &&
  (target.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName));

const CommandPalette = () => {
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const [lookups, setLookups] = useState<Lookups | null>(null);
  const [showExpense, setShowExpense] = useState(false);

  const loadLookups = async () => {
    if (lookups) return lookups;
    const [{ accounts }, stores, categories] = await Promise.all([
      getAccounts(),
      getAllStores(),
      getAllCategories(200, { for_accounts: false })
    ]);
    const next = { accounts: accounts.filter((account) => !account.is_closed), stores, categories };
    setLookups(next);
    return next;
  };

  const openExpense = async () => {
    setOpen(false);
    await loadLookups();
    setShowExpense(true);
  };

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault();
        setOpen((prev) => !prev);
        return;
      }
      if (
        event.key.toLowerCase() === 'n' &&
        !event.metaKey &&
        !event.ctrlKey &&
        !event.altKey &&
        !isTypingTarget(event.target) &&
        !document.querySelector('.modal.show, .modal[style*="block"]')
      ) {
        event.preventDefault();
        void openExpense();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  });

  useEffect(() => {
    if (open) void loadLookups();
  });

  const go = (path: string) => {
    setOpen(false);
    navigate(path);
  };

  return (
    <>
      <Command.Dialog
        open={open}
        onOpenChange={setOpen}
        label={labels.title}
        overlayClassName="palette-overlay"
        contentClassName="palette"
      >
        <Command.Input className="palette__input" placeholder={labels.placeholder} />
        <Command.List className="palette__list">
          <Command.Empty className="palette__empty">{labels.empty}</Command.Empty>
          <Command.Group heading={labels.actions}>
            <Command.Item className="palette__item" onSelect={() => void openExpense()}>
              <Plus size={16} aria-hidden="true" />
              {labels.addExpense}
              <kbd className="palette__kbd">N</kbd>
            </Command.Item>
          </Command.Group>
          <Command.Group heading={labels.screens}>
            {navGroups
              .flatMap((group) => group.links)
              .map(({ to, label, icon: Icon }) => (
                <Command.Item key={to} className="palette__item" onSelect={() => go(to)}>
                  <Icon size={16} aria-hidden="true" />
                  {label}
                </Command.Item>
              ))}
          </Command.Group>
          <Command.Group heading={labels.accounts}>
            {lookups?.accounts.map((account) => (
              <Command.Item
                key={account.id}
                value={`konto ${account.name}`}
                className="palette__item"
                onSelect={() => go(`/accounts/${account.id}`)}
              >
                <Wallet size={16} aria-hidden="true" />
                {account.name}
              </Command.Item>
            ))}
          </Command.Group>
          <Command.Group heading={labels.categories}>
            {lookups?.categories.map((category) => (
              <Command.Item
                key={category.id}
                value={`kategoria ${category.path || category.name}`}
                className="palette__item"
                onSelect={() => go('/budget')}
              >
                <PiggyBank size={16} aria-hidden="true" />
                {category.path || category.name}
              </Command.Item>
            ))}
          </Command.Group>
        </Command.List>
      </Command.Dialog>

      {lookups && (
        <NewTransactionModal
          open={showExpense}
          accounts={lookups.accounts}
          stores={lookups.stores}
          categories={lookups.categories}
          onClose={() => setShowExpense(false)}
          onCreated={async () => {
            toast.success(labels.expenseAdded);
            await refreshUntaggedCount();
          }}
        />
      )}
    </>
  );
};

export default CommandPalette;
