import { useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import { refreshUntaggedCount, useUntaggedCount } from '../hooks/useUntaggedCount';

/**
 * Liczba transakcji „do otagowania" przy linku w pasku bocznym.
 *
 * Odświeżana przy każdej zmianie strony: tag można nadać także w siatce
 * Transakcji czy przy paragonie, a COUNT na serwerze jest tańszy niż
 * pilnowanie każdego miejsca zapisu.
 */
const UntaggedNavBadge = () => {
  const count = useUntaggedCount();
  const { pathname } = useLocation();

  useEffect(() => {
    void refreshUntaggedCount();
  }, [pathname]);

  if (!count) return null;
  return (
    <span className="sidebar__badge" title={`${count} do otagowania`} aria-label={`${count} do otagowania`}>
      {count}
    </span>
  );
};

export default UntaggedNavBadge;
