import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
// Tabler/Bootstrap, wrapped in the lowest-priority @layer vendor (see vendor.css)
// so our own styles always win without UNLAYERED hacks. Must load first.
// (react-data-grid's stylesheet is imported into @layer vendor from vendor.css)
import './styles/vendor.css';
import '@fontsource-variable/inter/wght.css';

import { routerBasename } from './api/basePath';
import App from './App';
import './index.css';

ReactDOM.createRoot(document.getElementById('root') as HTMLElement).render(
  <React.StrictMode>
    <BrowserRouter basename={routerBasename()}>
      <App />
    </BrowserRouter>
  </React.StrictMode>
);
