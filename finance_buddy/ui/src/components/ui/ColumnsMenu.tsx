// Menu „Kolumny": włącza i wyłącza kolumny listy.
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import { Check, Columns3 } from 'lucide-react';
import { copy } from '../../copy/pl';

type ColumnsMenuProps = {
  columns: { key: string; label: string }[];
  hiddenKeys: ReadonlySet<string>;
  onChange: (key: string, visible: boolean) => void;
};

const ColumnsMenu = ({ columns, hiddenKeys, onChange }: ColumnsMenuProps) => (
  <DropdownMenu.Root>
    <DropdownMenu.Trigger className="text-button columns-menu__trigger">
      <Columns3 size={14} aria-hidden="true" />
      {copy.columnsMenu.label}
    </DropdownMenu.Trigger>
    <DropdownMenu.Portal>
      <DropdownMenu.Content className="menu" align="end" sideOffset={4}>
        {columns.map((column) => (
          <DropdownMenu.CheckboxItem
            key={column.key}
            className="menu__item menu__item--check"
            checked={!hiddenKeys.has(column.key)}
            onCheckedChange={(checked) => onChange(column.key, checked)}
            onSelect={(event) => event.preventDefault()}
          >
            <span className="menu__check">
              <DropdownMenu.ItemIndicator>
                <Check size={14} aria-hidden="true" />
              </DropdownMenu.ItemIndicator>
            </span>
            {column.label}
          </DropdownMenu.CheckboxItem>
        ))}
      </DropdownMenu.Content>
    </DropdownMenu.Portal>
  </DropdownMenu.Root>
);

export default ColumnsMenu;
