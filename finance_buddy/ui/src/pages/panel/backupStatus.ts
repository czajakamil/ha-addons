import type { BackupStatus } from '../../api';

export type BackupHealth = 'ok' | 'stale' | 'missing' | 'error' | 'disabled';

/**
 * Ocena stanu kopii na potrzeby plakietki. Kopia jest „nieaktualna", gdy od
 * ostatniej minęło wyraźnie więcej niż interwał — harmonogram sprawdza termin
 * co godzinę, więc dajemy mu półtora interwału zapasu.
 */
export const backupHealth = (status: BackupStatus, now: Date = new Date()): BackupHealth => {
  if (status.last_error) return 'error';
  if (!status.last_backup_at) return status.enabled ? 'missing' : 'disabled';
  const ageHours = (now.getTime() - new Date(status.last_backup_at).getTime()) / 3_600_000;
  if (ageHours > status.interval_hours * 1.5) return status.enabled ? 'stale' : 'disabled';
  return 'ok';
};

export const formatBytes = (bytes: number): string => {
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KB', 'MB', 'GB'];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toLocaleString('pl-PL', { maximumFractionDigits: 1 })} ${units[unit]}`;
};
