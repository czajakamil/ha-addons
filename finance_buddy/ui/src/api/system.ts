import { request, requestBlob } from './client';

export interface BackupFile {
  name: string;
  created_at: string;
  size_bytes: number;
}

export interface BackupStatus {
  enabled: boolean;
  directory: string;
  interval_hours: number;
  keep_daily: number;
  keep_weekly: number;
  keep_monthly: number;
  include_attachments: boolean;
  last_backup_at: string | null;
  next_backup_at: string | null;
  running: boolean;
  last_error: string | null;
  last_error_at: string | null;
  attachments_files: number;
  attachments_bytes: number;
  backups: BackupFile[];
}

export const getBackupStatus = () => request<BackupStatus>('/system/backups');

export const createBackupNow = () =>
  request<BackupFile>('/system/backups', { method: 'POST' });

export const downloadBackup = (name: string) =>
  requestBlob(`/system/backups/${encodeURIComponent(name)}`);
