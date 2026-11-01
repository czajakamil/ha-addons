// Konfiguracja ESLint (flat config) dla frontendu.
// Zakres celowo minimalny: TS + reguły hooków Reacta, bez reguł stylistycznych
// (formatowanie zostawiamy edytorowi), żeby lint zgłaszał tylko realne błędy.
import js from '@eslint/js';
import tseslint from 'typescript-eslint';
import reactHooks from 'eslint-plugin-react-hooks';
import reactRefresh from 'eslint-plugin-react-refresh';

export default tseslint.config(
  { ignores: ['dist', 'node_modules'] },
  {
    files: ['src/**/*.{ts,tsx}'],
    extends: [js.configs.recommended, ...tseslint.configs.recommended],
    languageOptions: {
      ecmaVersion: 2022,
      globals: {
        window: 'readonly',
        document: 'readonly',
        localStorage: 'readonly',
        fetch: 'readonly',
        console: 'readonly',
        navigator: 'readonly',
        FormData: 'readonly',
        Headers: 'readonly',
        RequestInit: 'readonly',
        KeyboardEvent: 'readonly',
        HTMLElement: 'readonly'
      }
    },
    plugins: {
      'react-hooks': reactHooks,
      'react-refresh': reactRefresh
    },
    rules: {
      ...reactHooks.configs.recommended.rules,
      'react-refresh/only-export-components': ['warn', { allowConstantExport: true }],
      // Nieużywane zmienne to błąd, ale świadome pominięcia oznaczamy podkreślnikiem.
      '@typescript-eslint/no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_' }
      ],
      // --- Dług adopcyjny (2026-07): poniższe reguły zgłaszają realne, ale
      // niegroźne problemy projektowe w istniejącym kodzie (~60 miejsc).
      // Trzymamy je jako ostrzeżenia, żeby lint był zielony i wyłapywał NOWE
      // błędy; podnieś do 'error' przy okazji refaktoru dużych stron.
      'react-hooks/set-state-in-effect': 'warn',
      'react-hooks/exhaustive-deps': 'warn',
      'react-hooks/preserve-manual-memoization': 'warn',
      '@typescript-eslint/no-explicit-any': 'warn',
      'no-useless-assignment': 'warn'
    }
  }
);
