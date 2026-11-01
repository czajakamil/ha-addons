import { request } from './client';

export interface DemoLoadResult {
  accounts: number;
  tags: number;
  stores: number;
  transactions: number;
}

export const loadDemoData = () => request<DemoLoadResult>('/demo/load', { method: 'POST' });
