import { useEffect, useRef, type ChangeEvent } from 'react';
import { CalendarIcon } from 'lucide-react';
import Litepicker from '@tabler/core/dist/libs/litepicker/dist/litepicker.umd.js';

const DATE_FORMAT = 'YYYY-MM-DD';

const pickerButtonIcons = {
  previousMonth:
    '<svg class="icon" xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M15 6l-6 6l6 6" /></svg>',
  nextMonth:
    '<svg class="icon" xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M9 6l6 6l-6 6" /></svg>'
};

type DatePickerFieldProps = {
  id: string;
  label: string;
  placeholder: string;
  value: string;
  onChange: (value: string) => void;
  className?: string;
};

const DatePickerField = ({
  id,
  label,
  placeholder,
  value,
  onChange,
  className
}: DatePickerFieldProps) => {
  const inputRef = useRef<HTMLInputElement | null>(null);
  const pickerRef = useRef<Litepicker | null>(null);
  const changeHandlerRef = useRef(onChange);

  useEffect(() => {
    changeHandlerRef.current = onChange;
  }, [onChange]);

  useEffect(() => {
    if (!inputRef.current) {
      return;
    }

    const picker = new Litepicker({
      element: inputRef.current,
      singleMode: true,
      autoApply: true,
      mobileFriendly: true,
      allowRepick: true,
      format: DATE_FORMAT,
      lang: 'pl-PL',
      buttonText: pickerButtonIcons
    });

    pickerRef.current = picker;

    const handleSelected = (selectedDate: { format?: (pattern: string) => string } | null) => {
      if (selectedDate?.format) {
        changeHandlerRef.current(selectedDate.format(DATE_FORMAT));
      }
    };

    picker.on('selected', handleSelected);
    picker.on('clear:selection', () => {
      changeHandlerRef.current('');
    });

    if (value) {
      picker.setDate(value);
    }

    return () => {
      picker.destroy();
      pickerRef.current = null;
    };
  }, []); // tylko raz

  useEffect(() => {
    const picker = pickerRef.current;
    const input = inputRef.current;

    if (!input) {
      return;
    }

    if (!picker) {
      input.value = value || '';
      return;
    }

    if (value) {
      picker.setDate(value);
    } else {
      picker.clearSelection();
      input.value = '';
    }
  }, [value]);

  const handleManualChange = (event: ChangeEvent<HTMLInputElement>) => {
    const nextValue = event.target.value.trim();
    changeHandlerRef.current(nextValue);
    if (!nextValue) {
      pickerRef.current?.clearSelection();
    }
  };

  return (
    <div className={className ?? 'col-md-3'}>
      <label className="form-label" htmlFor={id}>
        {label}
      </label>
      <div className="input-icon">
        <span className="input-icon-addon">
          <CalendarIcon size={16} aria-hidden="true" />
        </span>
        <input
          id={id}
          ref={inputRef}
          type="text"
          className="form-control"
          placeholder={placeholder}
          inputMode="numeric"
          autoComplete="off"
          value={value}
          onChange={handleManualChange}
        />
      </div>
    </div>
  );
};

export default DatePickerField;
