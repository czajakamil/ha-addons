import { useState } from 'react';
import { useIsMobile } from '../hooks';
import { Icon, type IconName } from '../components/Icon';
import { AdminUsersScreen } from './AdminUsersScreen';
import { AgentSettingsScreen } from './AgentSettingsScreen';
import { ApiKeysScreen } from './ApiKeysScreen';

type Section = 'agent' | 'users' | 'apikeys';

interface SettingsScreenProps {
  isAdmin: boolean;
  currentUserId: number;
}

interface SectionDef {
  id: Section;
  label: string;
  icon: IconName;
  adminOnly: boolean;
}

const SECTIONS: SectionDef[] = [
  { id: 'agent', label: 'Asystent AI', icon: 'cog', adminOnly: false },
  { id: 'users', label: 'Użytkownicy', icon: 'users', adminOnly: true },
  { id: 'apikeys', label: 'Klucze API', icon: 'key', adminOnly: true },
];

function renderSection(id: Section, isAdmin: boolean, currentUserId: number) {
  if (id === 'agent') return <AgentSettingsScreen />;
  if (id === 'users' && isAdmin) return <AdminUsersScreen currentUserId={currentUserId} />;
  if (id === 'apikeys' && isAdmin) return <ApiKeysScreen />;
  return null;
}

export function SettingsScreen({ isAdmin, currentUserId }: SettingsScreenProps) {
  const visible = SECTIONS.filter((s) => !s.adminOnly || isAdmin);
  const isMobile = useIsMobile();
  const [section, setSection] = useState<Section>('agent');

  if (isMobile) {
    return (
      <div className="settings-stack">
        {visible.map((s) => (
          <section key={s.id} className="settings-stack-section">
            {renderSection(s.id, isAdmin, currentUserId)}
          </section>
        ))}
      </div>
    );
  }

  return (
    <div className="settings-layout">
      <aside className="settings-nav">
        {visible.map((s) => (
          <button
            key={s.id}
            className={`settings-nav-item ${section === s.id ? 'active' : ''}`}
            onClick={() => setSection(s.id)}
          >
            <span className="ico">
              <Icon name={s.icon} size={16} />
            </span>
            <span>{s.label}</span>
          </button>
        ))}
      </aside>
      <div className="settings-content">{renderSection(section, isAdmin, currentUserId)}</div>
    </div>
  );
}
