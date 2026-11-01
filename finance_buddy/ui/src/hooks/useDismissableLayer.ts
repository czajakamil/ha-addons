import { RefObject, useEffect, useRef } from 'react';

type UseDismissableLayerOptions = {
  isOpen: boolean;
  layerRef: RefObject<HTMLElement>;
  triggerRef: RefObject<HTMLElement>;
  onDismiss: () => void;
  trapFocus?: boolean;
  restoreFocus?: boolean;
};

const focusableSelector = [
  'a[href]',
  'button:not([disabled])',
  'textarea:not([disabled])',
  'input:not([disabled])',
  'select:not([disabled])',
  '[tabindex]:not([tabindex="-1"])'
].join(',');

const getFocusableElements = (container: HTMLElement) =>
  Array.from(container.querySelectorAll<HTMLElement>(focusableSelector)).filter(
    (element) => !element.hasAttribute('disabled') && element.getAttribute('aria-hidden') !== 'true'
  );

export const useDismissableLayer = ({
  isOpen,
  layerRef,
  triggerRef,
  onDismiss,
  trapFocus = true,
  restoreFocus = true
}: UseDismissableLayerOptions) => {
  const wasOpenRef = useRef(false);

  useEffect(() => {
    if (!isOpen) {
      if (wasOpenRef.current && restoreFocus) {
        window.requestAnimationFrame(() => {
          triggerRef.current?.focus();
        });
      }
      wasOpenRef.current = false;
      return;
    }

    const layer = layerRef.current;
    if (!layer) {
      return;
    }

    wasOpenRef.current = true;

    const focusables = getFocusableElements(layer);
    const initialFocusTarget = focusables[0] ?? layer;
    window.requestAnimationFrame(() => {
      initialFocusTarget.focus();
    });

    const handlePointerDown = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (!target) return;
      if (layer.contains(target)) return;
      if (triggerRef.current?.contains(target)) return;
      onDismiss();
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault();
        onDismiss();
        return;
      }

      if (!trapFocus || event.key !== 'Tab') {
        return;
      }

      const currentFocusables = getFocusableElements(layer);
      if (!currentFocusables.length) {
        event.preventDefault();
        layer.focus();
        return;
      }

      const first = currentFocusables[0];
      const last = currentFocusables[currentFocusables.length - 1];
      const active = document.activeElement as HTMLElement | null;

      if (event.shiftKey && active === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && active === last) {
        event.preventDefault();
        first.focus();
      }
    };

    document.addEventListener('pointerdown', handlePointerDown);
    document.addEventListener('keydown', handleKeyDown);

    return () => {
      document.removeEventListener('pointerdown', handlePointerDown);
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [isOpen, layerRef, onDismiss, restoreFocus, trapFocus, triggerRef]);
};
