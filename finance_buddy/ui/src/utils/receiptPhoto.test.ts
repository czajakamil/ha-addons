import { describe, expect, it } from 'vitest';
import { cameraPhotoName, fitWithin, keepsOriginal, prepareReceiptFile, withJpegExtension } from './receiptPhoto';

const fileOf = (type: string, bytes: number, name = 'paragon') =>
  new File([new Uint8Array(bytes)], name, { type });

describe('fitWithin', () => {
  it('leaves images within the limit untouched', () => {
    expect(fitWithin(2300, 3000)).toEqual({ width: 2300, height: 3000 });
  });

  it('scales the longest edge down to the limit, keeping proportions', () => {
    expect(fitWithin(6000, 8000)).toEqual({ width: 2400, height: 3200 });
    expect(fitWithin(8064, 6048)).toEqual({ width: 3200, height: 2400 });
  });
});

describe('keepsOriginal', () => {
  it('keeps small JPEGs that already fit', () => {
    expect(keepsOriginal(fileOf('image/jpeg', 1_000_000), 2000, 3000)).toBe(true);
  });

  it('re-encodes big files, oversized images and HEIC', () => {
    expect(keepsOriginal(fileOf('image/jpeg', 6_000_000), 2000, 3000)).toBe(false);
    expect(keepsOriginal(fileOf('image/jpeg', 1_000_000), 4032, 3024)).toBe(false);
    expect(keepsOriginal(fileOf('image/heic', 1_000_000), 2000, 3000)).toBe(false);
  });
});

describe('file names', () => {
  it('names camera photos with a date the backend reads as the scan date', () => {
    expect(cameraPhotoName(new Date(2026, 10, 3, 9, 5))).toBe('zdjecie_2026-11-03_09-05.jpg');
  });

  it('swaps the extension to .jpg', () => {
    expect(withJpegExtension('IMG_0042.HEIC')).toBe('IMG_0042.jpg');
    expect(withJpegExtension('paragon')).toBe('paragon.jpg');
  });
});

describe('prepareReceiptFile', () => {
  it('passes PDFs through unchanged', async () => {
    const pdf = fileOf('application/pdf', 10, 'skan.pdf');
    expect(await prepareReceiptFile(pdf)).toBe(pdf);
  });

  it('falls back to the original when the browser cannot decode the image', async () => {
    // jsdom nie ma `createImageBitmap` — tak samo jak przeglądarka bez obsługi formatu.
    const heic = fileOf('image/heic', 10, 'IMG_0042.HEIC');
    expect(await prepareReceiptFile(heic)).toBe(heic);
  });
});
