// Wspólna rama okien Sklepów. Portal do <body>, bo `position: fixed` w transformowanym `.app__main` się przesuwa.
import { useEffect, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { X } from 'lucide-react';
import { copy } from '../../../copy/pl';

type StoreDialogProps = {
  title: string;
  onClose: () => void;
  footer: ReactNode;
  wide?: boolean;
  children: ReactNode;
};

const StoreDialog = ({ title, onClose, footer, wide = false, children }: StoreDialogProps) => {
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  return createPortal(
    <div className="stores-dialog__overlay">
      <div className={`stores-dialog${wide ? ' stores-dialog--wide' : ''}`} role="dialog" aria-modal="true" aria-labelledby="stores-dialog-title">
        <header className="stores-dialog__head">
          <h2 id="stores-dialog-title" className="stores-dialog__title">
            {title}
          </h2>
          <button type="button" className="accounts-menu-trigger stores-dialog__close" aria-label={copy.stores.close} onClick={onClose}>
            <X size={16} aria-hidden="true" />
          </button>
        </header>
        <div className="stores-dialog__body">{children}</div>
        <footer className="stores-dialog__foot">{footer}</footer>
      </div>
    </div>,
    document.body
  );
};

export default StoreDialog;
