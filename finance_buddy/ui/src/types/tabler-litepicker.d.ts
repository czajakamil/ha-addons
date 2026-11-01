declare module '@tabler/core/dist/libs/litepicker/dist/litepicker.umd.js' {
  export interface LitepickerOptions {
    element: HTMLElement | HTMLInputElement;
    format?: string;
    singleMode?: boolean;
    autoApply?: boolean;
    allowRepick?: boolean;
    mobileFriendly?: boolean;
    resetButton?: boolean | object;
    dropdowns?: {
      months?: boolean;
      years?: boolean;
      minYear?: number;
      maxYear?: number;
    };
    buttonText?: Record<string, string>;
    lang?: string;
  }

  export default class Litepicker {
    constructor(options: LitepickerOptions);
    on(event: string, callback: (...args: any[]) => void): void;
    setDate(date: string | Date, silent?: boolean): void;
    clearSelection(): void;
    destroy(): void;
  }
}
