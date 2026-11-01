const NIP_WEIGHTS = [6, 5, 7, 2, 3, 4, 5, 6, 7];

// Ta sama normalizacja co backend/finance_app/receipt_extraction/parsing.py::normalize_nip
export const normalizeNip = (raw: string | null | undefined): string | null => {
  const digits = (raw ?? '').replace(/\D/g, '');
  return digits.length === 10 ? digits : null;
};

// Ta sama suma kontrolna co backend/finance_app/helpers.py::is_valid_nip
export const isValidNip = (raw: string): boolean => {
  const digits = raw.replace(/\D/g, '');
  if (digits.length !== 10) return false;
  const checksum =
    digits
      .slice(0, 9)
      .split('')
      .reduce((sum, digit, index) => sum + Number(digit) * NIP_WEIGHTS[index], 0) % 11;
  return checksum !== 10 && checksum === Number(digits[9]);
};
