// Menu kontekstowe wiersza transakcji (otwórz szczegóły / usuń).
// Fokus wchodzi do menu po otwarciu; strzałki/Home/End przełączają pozycje,
// Enter/Spacja aktywują, Esc (obsługiwany na poziomie strony) zamyka.
import { useEffect, useRef, type KeyboardEvent as ReactKeyboardEvent } from 'react';
import { IconListDetails, IconTrash } from '@tabler/icons-react';

type TransactionsContextMenuProps = {
  x: number;
  y: number;
  onOpenDetails: () => void;
  onDelete: () => void;
};

const TransactionsContextMenu = ({ x, y, onOpenDetails, onDelete }: TransactionsContextMenuProps) => {
  const menuRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const firstItem = menuRef.current?.querySelector<HTMLElement>('[role="menuitem"]');
    firstItem?.focus();
  }, []);

  const handleKeyDown = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    const { key } = event;
    if (key !== 'ArrowDown' && key !== 'ArrowUp' && key !== 'Home' && key !== 'End') {
      return;
    }

    const items = Array.from(
      menuRef.current?.querySelectorAll<HTMLElement>('[role="menuitem"]') ?? []
    );
    if (items.length === 0) {
      return;
    }

    event.preventDefault();
    event.stopPropagation();

    const currentIndex = items.indexOf(document.activeElement as HTMLElement);
    const nextIndex =
      key === 'Home'
        ? 0
        : key === 'End'
          ? items.length - 1
          : key === 'ArrowDown'
            ? (currentIndex + 1 + items.length) % items.length
            : (currentIndex - 1 + items.length) % items.length;

    items[nextIndex]?.focus();
  };

  return (
    <div
      ref={menuRef}
      className="transactions-context-menu"
      style={{ top: `${y}px`, left: `${x}px` }}
      role="menu"
      aria-label="Menu transakcji"
      onClick={(event) => event.stopPropagation()}
      onContextMenu={(event) => event.preventDefault()}
      onKeyDown={handleKeyDown}
    >
      <div className="transactions-context-menu__grid">
        <button
          type="button"
          role="menuitem"
          className="transactions-context-menu__item is-focusable u-inline-center"
          aria-label="Otwórz szczegóły"
          onClick={onOpenDetails}
        >
          <IconListDetails
            className="transactions-context-menu__icon u-inline-center"
            size={18}
            aria-hidden="true"
          />
          <span className="transactions-context-menu__label">Pokaż szczegóły</span>
        </button>
        <button
          type="button"
          role="menuitem"
          className="transactions-context-menu__item transactions-context-menu__item--danger is-focusable u-inline-center"
          aria-label="Usuń"
          onClick={onDelete}
        >
          <IconTrash
            className="transactions-context-menu__icon u-inline-center"
            size={18}
            aria-hidden="true"
          />
          <span className="transactions-context-menu__label">Usuń</span>
        </button>
      </div>
    </div>
  );
};

export default TransactionsContextMenu;
