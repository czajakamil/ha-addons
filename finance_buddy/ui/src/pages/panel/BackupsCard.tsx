import { useState } from 'react';
import { DatabaseBackup, Download, RefreshCw } from 'lucide-react';
import { toast } from 'sonner';
import { useApi } from '../../hooks/useApi';
import { createBackupNow, downloadBackup, getBackupStatus, type BackupStatus } from '../../api';
import { backupHealth, formatBytes, type BackupHealth } from './backupStatus';
import { errorDetail, formatDateTime } from './format';
import ResizableTable from '../../components/ui/ResizableTable';

const HEALTH_BADGE: Record<BackupHealth, { className: string; label: string }> = {
  ok: { className: 'panel-badge--positive', label: 'Aktualna' },
  stale: { className: 'panel-badge--warning', label: 'Nieaktualna' },
  missing: { className: 'panel-badge--warning', label: 'Brak kopii' },
  error: { className: 'panel-badge--negative', label: 'Błąd' },
  disabled: { className: '', label: 'Wyłączone' }
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

  const handleCreate = async () => {
    setCreating(true);
    try {
      const backup = await createBackupNow();
      toast(`Zapisano kopię ${backup.name}`);
    } catch (err) {
      toast.error(errorDetail(err, 'Nie udało się zrobić kopii.'));
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
      toast.error(errorDetail(err, 'Nie udało się pobrać kopii.'));
    } finally {
      setDownloading(null);
    }
  };

  const health = data ? HEALTH_BADGE[backupHealth(data)] : null;

  return (
    <section className="panel-section">
      <div className="panel-section__head">
        <h2 className="panel-section__title">
          Kopie zapasowe
          {health && <span className={`panel-badge ${health.className}`}>{health.label}</span>}
        </h2>
        <div className="panel-section__actions">
          <button
            className="account-button"
            type="button"
            onClick={() => void refetch()}
            disabled={loading}
          >
            <RefreshCw size={14} aria-hidden="true" />
            Odśwież
          </button>
          <button
            className="account-button"
            type="button"
            onClick={() => void handleCreate()}
            disabled={creating || Boolean(data?.running)}
          >
            <DatabaseBackup size={14} aria-hidden="true" />
            {creating ? 'Tworzę kopię…' : 'Zrób kopię teraz'}
          </button>
        </div>
      </div>

      {error && (
        <div className="alert alert-danger mb-0" role="alert">
          {error}
        </div>
      )}

      {data?.last_error && (
        <div className="alert alert-danger mb-0" role="alert">
          Ostatnia próba ({formatDateTime(data.last_error_at)}) nie powiodła się: {data.last_error}
        </div>
      )}

      <div className="account-settings__card">
        {loading && !data ? (
          <div className="placeholder-glow">
            <div className="placeholder col-6 mb-2" />
            <div className="placeholder col-8" />
          </div>
        ) : data ? (
          <dl className="panel-facts">
            <div className="account-settings__row">
              <dt className="account-field-label">Ostatnia kopia</dt>
              <dd className="panel-text panel-numbers">{formatDateTime(data.last_backup_at)}</dd>
            </div>
            <div className="account-settings__row">
              <dt className="account-field-label">Następna automatyczna</dt>
              <dd className="panel-text panel-numbers">
                {data.enabled
                  ? `${formatDateTime(data.next_backup_at)} (co ${data.interval_hours} h)`
                  : 'Automatyczne kopie wyłączone (FINANCE_BACKUP_ENABLED)'}
              </dd>
            </div>
            <div className="account-settings__row">
              <dt className="account-field-label">Katalog</dt>
              <dd className="panel-text panel-break">{data.directory}</dd>
            </div>
            <div className="account-settings__row">
              <dt className="account-field-label">Rotacja</dt>
              <dd className="panel-text panel-numbers">
                Dzienne: {data.keep_daily}, tygodniowe: {data.keep_weekly}, miesięczne: {data.keep_monthly}
              </dd>
            </div>
            <div className="account-settings__row">
              <dt className="account-field-label">Załączniki</dt>
              <dd className="panel-text panel-numbers">
                {data.include_attachments
                  ? `Pliki: ${data.attachments_files}, ${formatBytes(data.attachments_bytes)} (wspólne lustro)`
                  : 'Nie są kopiowane (FINANCE_BACKUP_ATTACHMENTS)'}
              </dd>
            </div>
          </dl>
        ) : null}
      </div>

      {data && data.backups.length > 0 && (
        <div className="panel-table-card">
          <div className="table-responsive">
            <ResizableTable tableId="backups" className="table table-vcenter mb-0 panel-table">
              <thead>
                <tr>
                  <th>Kopia bazy</th>
                  <th className="panel-hide-phone">Utworzona</th>
                  <th className="text-end">Rozmiar</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {data.backups.map((backup) => (
                  <tr key={backup.name}>
                    <td className="panel-break">{backup.name}</td>
                    <td className="panel-numbers panel-hide-phone">{formatDateTime(backup.created_at)}</td>
                    <td className="text-end panel-numbers">{formatBytes(backup.size_bytes)}</td>
                    <td className="panel-table__menu">
                      <button
                        className="account-button panel-icon-button"
                        type="button"
                        onClick={() => void handleDownload(backup.name)}
                        disabled={downloading === backup.name}
                        aria-label={`Pobierz plik bazy ${backup.name}`}
                        title="Pobierz plik bazy (bez załączników)"
                      >
                        <Download size={14} aria-hidden="true" />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </ResizableTable>
          </div>
        </div>
      )}
    </section>
  );
};

export default BackupsCard;
