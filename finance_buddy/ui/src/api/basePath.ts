/**
 * Ścieżki pod Home Assistant Ingress, gdzie apka żyje pod
 * `/api/hassio_ingress/<token>/`. Prefiks zna dopiero nginx, który wstrzykuje go
 * do `index.html` jako `<base href>`. Bez tego tagu (Docker, desktop, dev) obie
 * funkcje zachowują się tak, jakby prefiksu nie było.
 */

/** Względny adres API (np. `__api`) rozwiązuje wobec `<base>`, pozostałe zostawia. */
export function resolveApiBaseUrl(configured: string): string {
  if (/^([a-z]+:)?\//i.test(configured)) {
    return configured;
  }
  return new URL(configured, document.baseURI).pathname;
}

/** `basename` dla routera: prefiks z `<base>` bez końcowego ukośnika. */
export function routerBasename(): string | undefined {
  if (!document.querySelector('base[href]')) {
    return undefined;
  }
  return new URL(document.baseURI).pathname.replace(/\/$/, '') || undefined;
}
