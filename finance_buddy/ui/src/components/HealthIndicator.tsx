import { useEffect, useState } from 'react';
import { getHealth } from '../api';

type HealthStatus = 'loading' | 'ok' | 'error';

const HealthIndicator = () => {
  const [status, setStatus] = useState<HealthStatus>('loading');

  useEffect(() => {
    let isMounted = true;

    const fetchHealth = async () => {
      try {
        await getHealth();
        if (isMounted) {
          setStatus('ok');
        }
      } catch (error) {
        console.error('Health check failed', error);
        if (isMounted) {
          setStatus('error');
        }
      }
    };

    fetchHealth();
    const interval = window.setInterval(fetchHealth, 20000);

    return () => {
      isMounted = false;
      window.clearInterval(interval);
    };
  }, []);

  if (status === 'loading') {
    return <span className="badge">Checking...</span>;
  }

  if (status === 'error') {
    return <span className="badge badge--error">API offline</span>;
  }

  return <span className="badge badge--ok">API online</span>;
};

export default HealthIndicator;
