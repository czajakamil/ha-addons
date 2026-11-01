import { ReactNode, Ref, useRef } from 'react';
import { useDismissableLayer } from '../hooks/useDismissableLayer';

type PopoverFilterTriggerRenderProps = {
  ref: Ref<HTMLButtonElement>;
  panelId: string;
  isOpen: boolean;
  onToggle: () => void;
};

type PopoverFilterProps = {
  id: string;
  label: string;
  isOpen: boolean;
  className?: string;
  panelClassName: string;
  onOpenChange: (nextOpen: boolean) => void;
  trigger: (props: PopoverFilterTriggerRenderProps) => ReactNode;
  children: ReactNode;
};

const PopoverFilter = ({
  id,
  label,
  isOpen,
  className,
  panelClassName,
  onOpenChange,
  trigger,
  children
}: PopoverFilterProps) => {
  const layerRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const panelId = `${id}-panel`;

  useDismissableLayer({
    isOpen,
    layerRef,
    triggerRef,
    onDismiss: () => onOpenChange(false)
  });

  return (
    <div className={className}>
      {trigger({
        ref: triggerRef,
        panelId,
        isOpen,
        onToggle: () => onOpenChange(!isOpen)
      })}
      {isOpen && (
        <div
          id={panelId}
          ref={layerRef}
          className={panelClassName}
          role="dialog"
          aria-label={label}
          tabIndex={-1}
        >
          {children}
        </div>
      )}
    </div>
  );
};

export default PopoverFilter;
