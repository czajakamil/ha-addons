import { describe, expect, it } from 'vitest';
import { isMacEmbeddedWebView } from './DateTimeInput';

const WEBKIT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko)';

describe('isMacEmbeddedWebView', () => {
  it('wykrywa WebView na Macu, także z dopiskiem aplikacji', () => {
    expect(isMacEmbeddedWebView(WEBKIT, 0)).toBe(true);
    expect(isMacEmbeddedWebView(`${WEBKIT} Home Assistant/2026.10.3`, 0)).toBe(true);
  });

  it('zostawia natywny wybierak przeglądarkom i iPadowi', () => {
    expect(isMacEmbeddedWebView(`${WEBKIT} Version/26.0 Safari/605.1.15`, 0)).toBe(false);
    expect(isMacEmbeddedWebView('Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:140.0) Gecko/20100101 Firefox/140.0', 0)).toBe(false);
    expect(isMacEmbeddedWebView(WEBKIT, 5)).toBe(false);
  });
});
