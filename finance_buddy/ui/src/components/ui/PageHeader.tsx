// Nagłówek ekranu: tytuł z licznikiem po lewej, akcje po prawej.
import type { ReactNode } from 'react';

type PageHeaderProps = {
  title: string;
  count?: number;
  actions?: ReactNode;
};

const PageHeader = ({ title, count, actions }: PageHeaderProps) => (
  <header className="page-heading">
    <h1 className="page-heading__title">
      {title}
      {count !== undefined && <span className="page-heading__count">{count}</span>}
    </h1>
    {actions && <div className="page-heading__actions">{actions}</div>}
  </header>
);

export default PageHeader;
