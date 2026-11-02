// Dymek wykresu. Leży w <body>, bo tabela i karty przycinają to, co wystaje poza ich krawędź.
import { ReactNode, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';

const GAP_PX = 8;
const EDGE_PX = 8;

type ChartTooltipProps = {
  anchor: DOMRect;
  /** Obok kotwicy zamiast nad nią: dla wysokich słupków, nad którymi nie ma miejsca. */
  beside?: boolean;
  onDismiss: () => void;
  children: ReactNode;
};

const ChartTooltip = ({ anchor, beside = false, onDismiss, children }: ChartTooltipProps) => {
  const ref = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState<{ left: number; top: number } | null>(null);

  // Rozmiar znamy dopiero po wyrenderowaniu; dymek nie może wyjechać poza ekran
  useLayoutEffect(() => {
    const element = ref.current;
    if (!element) return;
    const { width, height } = element.getBoundingClientRect();
    if (beside) {
      const right = anchor.right + GAP_PX;
      const fitsRight = right + width <= window.innerWidth - EDGE_PX;
      const middle = anchor.top + anchor.height / 2 - height / 2;
      setPosition({
        left: fitsRight ? right : Math.max(anchor.left - width - GAP_PX, EDGE_PX),
        top: Math.min(Math.max(middle, EDGE_PX), window.innerHeight - height - EDGE_PX)
      });
      return;
    }
    const centered = anchor.left + anchor.width / 2 - width / 2;
    const above = anchor.top - height - GAP_PX;
    setPosition({
      left: Math.min(Math.max(centered, EDGE_PX), window.innerWidth - width - EDGE_PX),
      top: above >= EDGE_PX ? above : anchor.bottom + GAP_PX
    });
  }, [anchor, beside, children]);

  // Pozycja jest liczona względem ekranu, więc po przewinięciu wskazywałaby nie ten słupek
  useEffect(() => {
    window.addEventListener('scroll', onDismiss, { capture: true, passive: true });
    return () => window.removeEventListener('scroll', onDismiss, { capture: true });
  }, [onDismiss]);

  return createPortal(
    <div
      ref={ref}
      className="report-tooltip chart-tooltip"
      role="tooltip"
      style={position ?? { left: 0, top: 0, visibility: 'hidden' }}
    >
      {children}
    </div>,
    document.body
  );
};

export default ChartTooltip;
