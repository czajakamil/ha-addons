import { useState } from 'react';
import { DatabaseBackupIcon, DownloadIcon, RefreshCwIcon } from 'lucide-react';
import { useApi } from '../../hooks/useApi';
import { createBackupNow, downloadBackup, getBackupStatus, type BackupStatus } from '../../api';
import { backupHealth, formatBytes, type BackupHealth } from './backupStatus';

const HEALTH_BADGE: Record<BackupHealth, { className: string; label: string }> = {
  ok: { className: 'bg-green-lt', label: 'Aktualna' },
  stale: { className: 'bg-yellow-lt', label: 'Nieaktualna' },
  missing: { className: 'bg-yellow-lt', label: 'Brak kopii' },
  error: { className: 'bg-red-lt', label: 'Błąd' },
  disabled: { className: 'bg-secondary-lt', label: 'Wyłączone' }
};

const formatDateTime = (value: string | null) =>
  value ? new Date(value).toLocaleString('pl-PL') : '—';

const errorDetail = (err: unknown, fallback: string) => {
  if (!(err instanceof Error)) return fallback;
  try {
    const parsed = JSON.parse(err.message) as { detail?: string };
    return parsed.detail ?? err.message;
  } catch {
    return err.message || fallback;
  }
};

const saveBlob = (blob: Blob, filename: string) => {
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
};

/** Stan automatycznych kopii bazy i przycisk „zrób kopię teraz" (tylko admin). */
const BackupsCard = () => {
  const { data, loading, error, refetch } = useApi<BackupStatus>(getBackupStatus, {
    errorMessage: 'Nie udało się pobrać stanu kopii zapasowych.'
  });
  const [creating, setCreating] = useState(false);
  const [downloading, setDownloading] = useState<string | null>(null);
  const [message, setMessage] = useState<{ ok: boolean; text: string } | null>(null);

  const handleCreate = async () => {
    setCreating(true);
    setMessage(null);
    try {
      const backup = await createBackupNow();
      setMessage({ ok: true, text: `Zapisano kopię ${backup.name}.` });
    } catch (err) {
      setMessage({ ok: false, text: errorDetail(err, 'Nie udało się zrobić kopii.') });
    } finally {
      setCreating(false);
      void refetch();
    }
  };

  const handleDownload = async (name: string) => {
    setDownloading(name);
    try {
      saveBlob(await downloadBackup(name), name);
    } catch (err) {
      setMessage({ ok: false, text: errorDetail(err, 'Nie udało się pobrać kopii.') });
    } finally {
      setDownloading(null);
    }
  };

  const health = data ? HEALTH_BADGE[backupHealth(data)] : null;

  return (
    <div className="card mb-3">
      <div className="card-header d-flex align-items-center gap-2">
        <h3 className="card-title mb-0">Kopie zapasowe</h3>
        {health && <span className={`badge ${health.className}`}>{health.label}</span>}
        <div className="ms-auto d-flex gap-2">
          <button
            className="btn btn-outline-secondary btn-sm"
            type="button"
            onClick={() => void refetch()}
            disabled={loading}
          >
            <RefreshCwIcon size={16} className="me-1" />
            Odśwież
          </button>
          <button
            className="btn btn-primary btn-sm"
            type="button"
            onClick={() => void handleCreate()}
            disabled={creating || Boolean(data?.running)}
          >
            <DatabaseBackupIcon size={16} className="me-1" />
            {creating ? 'Tworzę kopię…' : 'Zrób kopię teraz'}
          </button>
        </div>
      </div>
      <div className="card-body">
        {error && <div className="alert alert-danger">{error}</div>}
        {loading && !data ? (
          <div className="placeholder-glow">
            <div className="placeholder col-6 mb-2" />
            <div className="placeholder col-8" />
          </div>
        ) : data ? (
          <>
            <dl className="row mb-0">
              <dt className="col-sm-4">Ostatnia kopia</dt>
              <dd className="col-sm-8">{formatDateTime(data.last_backup_at)}</dd>
              <dt className="col-sm-4">Następna automatyczna</dt>
              <dd className="col-sm-8">
                {data.enabled
                  ? `${formatDateTime(data.next_backup_at)} (co ${data.interval_hours} h)`
                  : 'Automatyczne kopie wyłączone (FINANCE_BACKUP_ENABLED)'}
              </dd>
              <dt className="col-sm-4">Katalog</dt>
              <dd className="col-sm-8">
                <code className="text-break">{data.directory}</code>
              </dd>
              <dt className="col-sm-4">Rotacja</dt>
              <dd className="col-sm-8">
                {data.keep_daily} dziennych, {data.keep_weekly} tygodniowych,{' '}
                {data.keep_monthly} miesięcznych
              </dd>
              <dt className="col-sm-4">Załączniki</dt>
              <dd className="col-sm-8">
                {data.include_attachments
                  ? `${data.attachments_files} plików, ${formatBytes(data.attachments_bytes)} (wspólne lustro)`
                  : 'Nie są kopiowane (FINANCE_BACKUP_ATTACHMENTS)'}
              </dd>
            </dl>
            {data.last_error && (
              <div className="alert alert-danger mt-3 mb-0">
                Ostatnia próba ({formatDateTime(data.last_error_at)}) nie powiodła się:{' '}
                {data.last_error}
              </div>
            )}
          </>
        ) : null}
        {message && (
          <div className={`alert mt-3 mb-0 ${message.ok ? 'alert-success' : 'alert-danger'}`}>
            {message.text}
          </div>
        )}
      </div>
      {data && data.backups.length > 0 && (
        <div className="table-responsive">
          <table className="table table-vcenter card-table mb-0">
            <thead>
              <tr>
                <th>Kopia bazy</th>
                <th>Utworzona</th>
                <th className="text-end">Rozmiar</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {data.backups.map((backup) => (
                <tr key={backup.name}>
                  <td>
                    <code>{backup.name}</code>
                  </td>
                  <td>{formatDateTime(backup.created_at)}</td>
                  <td className="text-end">{formatBytes(backup.size_bytes)}</td>
                  <td className="text-end">
                    <button
                      className="btn btn-outline-primary btn-sm"
                      type="button"
                      onClick={() => void handleDownload(backup.name)}
                      disabled={downloading === backup.name}
                      title="Pobierz plik bazy (bez załączników)"
                    >
                      <DownloadIcon size={16} className="me-1" />
                      Pobierz
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
};

export default BackupsCard;
