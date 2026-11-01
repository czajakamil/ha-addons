// Pusty stan: ikona, jedno zdanie i jedna akcja.
import type { LucideIcon } from 'lucide-react';
import type { ReactNode } from 'react';

type EmptyStateProps = {
  icon: LucideIcon;
  message: string;
  action?: ReactNode;
};

const EmptyState = ({ icon: Icon, message, action }: EmptyStateProps) => (
  <div className="empty-block">
    <Icon size={24} strokeWidth={1.5} aria-hidden="true" />
    <p className="empty-block__message">{message}</p>
    {action}
  </div>
);

export default EmptyState;
