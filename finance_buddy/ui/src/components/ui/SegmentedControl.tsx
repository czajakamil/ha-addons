// Segmentowany przełącznik z opcjonalnymi licznikami.
type SegmentedOption<T extends string> = {
  value: T;
  label: string;
  count?: number;
};

type SegmentedControlProps<T extends string> = {
  label: string;
  value: T;
  options: SegmentedOption<T>[];
  onChange: (value: T) => void;
};

const SegmentedControl = <T extends string>({ label, value, options, onChange }: SegmentedControlProps<T>) => (
  <div className="segmented" role="group" aria-label={label}>
    {options.map((option) => (
      <button
        key={option.value}
        type="button"
        className={`segmented__option${option.value === value ? ' is-active' : ''}`}
        aria-pressed={option.value === value}
        onClick={() => onChange(option.value)}
      >
        <span>{option.label}</span>
        {option.count !== undefined && <span className="segmented__count">{option.count}</span>}
      </button>
    ))}
  </div>
);

export default SegmentedControl;
