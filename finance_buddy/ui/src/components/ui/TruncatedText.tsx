// Tekst ucinany elipsą. Tooltip z pełną treścią pojawia się tylko, gdy tekst faktycznie się nie mieści.
import { useRef, useState } from 'react';
import * as Tooltip from '@radix-ui/react-tooltip';

type TruncatedTextProps = {
  text: string;
  className?: string;
};

const TruncatedText = ({ text, className }: TruncatedTextProps) => {
  const ref = useRef<HTMLSpanElement>(null);
  const [open, setOpen] = useState(false);

  const handleOpenChange = (next: boolean) => {
    const element = ref.current;
    setOpen(next && element !== null && element.scrollWidth > element.clientWidth);
  };

  return (
    <Tooltip.Root open={open} onOpenChange={handleOpenChange}>
      <Tooltip.Trigger asChild>
        <span ref={ref} className={['truncate', className].filter(Boolean).join(' ')}>
          {text}
        </span>
      </Tooltip.Trigger>
      <Tooltip.Portal>
        <Tooltip.Content className="tooltip-content" sideOffset={4}>
          {text}
        </Tooltip.Content>
      </Tooltip.Portal>
    </Tooltip.Root>
  );
};

export default TruncatedText;
