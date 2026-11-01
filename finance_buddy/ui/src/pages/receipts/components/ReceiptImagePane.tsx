import { useEffect, useState } from 'react';
import { IconFileText, IconMinus, IconPlus, IconZoomScan } from '@tabler/icons-react';
import Button from '../../../components/Button';
import { fetchReceiptDraftImage } from '../../../api';

type ReceiptImagePaneProps = {
  draftId: string;
  hasImage: boolean;
  label: string;
};

const ZOOM_STEP = 0.25;
const ZOOM_MIN = 0.4;
const ZOOM_MAX = 4;

/**
 * Obraz paragonu obok formularza, z zoomem i „dopasuj".
 *
 * Bez tego przepisywanie ze skanu jest zgadywaniem — miniatura w karcie
 * wystarczała, żeby rozpoznać paragon, ale nie żeby odczytać z niego kwoty.
 *
 * Obraz pobieramy fetchem do bloba, bo endpoint wymaga nagłówka z tokenem,
 * a `<img src>` nagłówków nie wysyła.
 */
const ReceiptImagePane = ({ draftId, hasImage, label }: ReceiptImagePaneProps) => {
  const [src, setSrc] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [zoom, setZoom] = useState(1);
  const [fitMode, setFitMode] = useState(true);

  useEffect(() => {
    if (!hasImage) {
      setSrc(null);
      return;
    }
    let objectUrl: string | null = null;
    let cancelled = false;
    setError(null);
    void (async () => {
      try {
        const url = await fetchReceiptDraftImage(draftId);
        objectUrl = url;
        if (cancelled) {
          URL.revokeObjectURL(url);
          return;
        }
        setSrc(url);
      } catch {
        if (!cancelled) setError('Nie udało się wczytać obrazu paragonu.');
      }
    })();
    return () => {
      cancelled = true;
      if (objectUrl) URL.revokeObjectURL(objectUrl);
    };
  }, [draftId, hasImage]);

  // Nowy paragon zawsze otwiera się dopasowany do panelu — zoom z poprzedniego
  // skanu nic nie mówi o tym, w którą część tego trzeba się wpatrzeć.
  useEffect(() => {
    setZoom(1);
    setFitMode(true);
  }, [draftId]);

  const stepZoom = (direction: 1 | -1) => {
    setFitMode(false);
    setZoom((current) =>
      Math.min(ZOOM_MAX, Math.max(ZOOM_MIN, current + direction * ZOOM_STEP))
    );
  };

  return (
    <div className="receipt-pane">
      <div className="receipt-pane__toolbar">
        <span className="text-secondary text-truncate">{label}</span>
        <span className="flex-grow-1" />
        <Button
          variant="outline-secondary"
          size="sm"
          icon={<IconMinus size={16} />}
          onClick={() => stepZoom(-1)}
          aria-label="Pomniejsz"
        >
          <span className="visually-hidden">Pomniejsz</span>
        </Button>
        <span className="receipt-pane__zoom">
          {fitMode ? 'dopasuj' : `${Math.round(zoom * 100)}%`}
        </span>
        <Button
          variant="outline-secondary"
          size="sm"
          icon={<IconPlus size={16} />}
          onClick={() => stepZoom(1)}
          aria-label="Powiększ"
        >
          <span className="visually-hidden">Powiększ</span>
        </Button>
        <Button
          variant="outline-secondary"
          size="sm"
          icon={<IconZoomScan size={16} />}
          onClick={() => {
            setFitMode(true);
            setZoom(1);
          }}
        >
          Dopasuj
        </Button>
      </div>

      <div className={`receipt-pane__viewport${fitMode ? ' receipt-pane__viewport--fit' : ''}`}>
        {src ? (
          <img
            src={src}
            alt={`Paragon: ${label}`}
            style={fitMode ? undefined : { width: `${Math.round(zoom * 100)}%` }}
          />
        ) : (
          <div className="receipt-pane__empty text-secondary">
            <IconFileText size={20} />
            <span>{error ?? (hasImage ? 'Wczytywanie obrazu…' : 'Ten szkic nie ma obrazu.')}</span>
          </div>
        )}
      </div>
    </div>
  );
};

export default ReceiptImagePane;
