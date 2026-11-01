// Kolorowy kafelek z ikoną — jedno źródło wyglądu awatara konta (lista + modale).
import { createElement, type CSSProperties } from 'react';
import { getAccountColorValue, getAccountIcon } from '../accountPresentation';

type AccountAvatarProps = {
  icon?: string | null;
  color?: string | null;
  size?: 'sm' | 'md';
};

const AccountAvatar = ({ icon, color, size = 'md' }: AccountAvatarProps) => {
  const style = { '--account-avatar-color': getAccountColorValue(color) } as CSSProperties;
  return (
    <span className={`account-avatar account-avatar--${size}`} style={style} aria-hidden="true">
      {createElement(getAccountIcon(icon), { size: size === 'sm' ? 16 : 18, strokeWidth: 1.7 })}
    </span>
  );
};

export default AccountAvatar;
