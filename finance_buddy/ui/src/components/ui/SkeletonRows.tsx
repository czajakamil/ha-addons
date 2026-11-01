// Szkielet wierszy tabeli na czas ładowania.
type SkeletonRowsProps = {
  rows?: number;
};

const SkeletonRows = ({ rows = 10 }: SkeletonRowsProps) => (
  <div className="skeleton-rows" aria-hidden="true">
    {Array.from({ length: rows }, (_, index) => (
      <div key={index} className="skeleton-rows__row">
        <span className="skeleton skeleton--name" />
        <span className="skeleton skeleton--amount" />
        <span className="skeleton skeleton--amount" />
        <span className="skeleton skeleton--amount" />
      </div>
    ))}
  </div>
);

export default SkeletonRows;
