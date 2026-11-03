/**
 * Zdjęcia paragonów przed wysyłką.
 *
 * Fotka z telefonu ma 12–48 Mpx i 5–12 MB, a backend i tak tnie ją do 4000 px
 * przed OCR. Zmniejszenie w przeglądarce skraca wysyłkę przez komórkowy internet
 * kilkukrotnie i przy okazji zamienia HEIC (którego Pillow nie czyta) na JPEG.
 * 3200 px na dłuższym boku to wciąż więcej niż zwykły skan w 200 DPI (~2300 px),
 * więc jakość OCR nie spada.
 */
export const MAX_PHOTO_EDGE_PX = 3200;
const JPEG_QUALITY = 0.9;
// Mały JPEG/PNG wysyłamy bez zmian: ponowna kompresja tylko by go pogorszyła.
const KEEP_ORIGINAL_MAX_BYTES = 3 * 1024 * 1024;
const KEEP_ORIGINAL_TYPES = new Set(['image/jpeg', 'image/png', 'image/webp']);

export const fitWithin = (width: number, height: number, maxEdge = MAX_PHOTO_EDGE_PX) => {
  const longest = Math.max(width, height);
  if (longest <= maxEdge) return { width, height };
  const scale = maxEdge / longest;
  return { width: Math.round(width * scale), height: Math.round(height * scale) };
};

export const keepsOriginal = (file: File, width: number, height: number) =>
  KEEP_ORIGINAL_TYPES.has(file.type) &&
  file.size <= KEEP_ORIGINAL_MAX_BYTES &&
  Math.max(width, height) <= MAX_PHOTO_EDGE_PX;

const pad = (value: number) => String(value).padStart(2, '0');

/**
 * Nazwa dla zdjęcia prosto z aparatu — iOS nazywa każde `image.jpg`, więc w skrzynce
 * nie dałoby się ich odróżnić. Data w formacie RRRR-MM-DD jest też granicą daty
 * zakupu, którą backend czyta z nazwy pliku.
 */
export const cameraPhotoName = (now = new Date()) =>
  `zdjecie_${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}_${pad(now.getHours())}-${pad(
    now.getMinutes()
  )}.jpg`;

export const withJpegExtension = (name: string) => `${name.replace(/\.[^./\\]+$/, '') || 'zdjecie'}.jpg`;

// `createImageBitmap` domyślnie stosuje orientację z EXIF, więc obrót zostaje
// „wypalony” w pikselach i nie ginie razem z metadanymi.
const decode = async (file: File): Promise<ImageBitmap | null> => {
  if (typeof createImageBitmap !== 'function') return null;
  try {
    return await createImageBitmap(file, { imageOrientation: 'from-image' });
  } catch {
    return null;
  }
};

const encodeJpeg = (canvas: HTMLCanvasElement) =>
  new Promise<Blob | null>((resolve) => canvas.toBlob(resolve, 'image/jpeg', JPEG_QUALITY));

/**
 * Zwraca plik gotowy do wysyłki. Przy każdym kłopocie (PDF, format, którego
 * przeglądarka nie rozkoduje, brak canvasa) oddaje oryginał — niech backend
 * spróbuje albo zwróci czytelny błąd.
 */
export const prepareReceiptFile = async (file: File, name = file.name): Promise<File> => {
  if (!file.type.startsWith('image/')) return file;

  const bitmap = await decode(file);
  if (!bitmap) return file;

  try {
    if (keepsOriginal(file, bitmap.width, bitmap.height)) {
      return name === file.name ? file : new File([file], name, { type: file.type, lastModified: file.lastModified });
    }

    const target = fitWithin(bitmap.width, bitmap.height);
    const canvas = document.createElement('canvas');
    canvas.width = target.width;
    canvas.height = target.height;
    const context = canvas.getContext('2d');
    if (!context) return file;
    context.drawImage(bitmap, 0, 0, target.width, target.height);

    const blob = await encodeJpeg(canvas);
    if (!blob) return file;
    return new File([blob], withJpegExtension(name), { type: 'image/jpeg', lastModified: file.lastModified });
  } finally {
    bitmap.close();
  }
};
