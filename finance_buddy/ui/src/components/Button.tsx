import type { ButtonHTMLAttributes, ReactNode } from 'react';

type ButtonVariant =
  | 'primary'
  | 'secondary'
  | 'outline-primary'
  | 'outline-secondary'
  | 'light'
  | 'link';

type ButtonSize = 'sm' | 'md';

type TablerButtonProps = {
  icon?: ReactNode;
  iconPosition?: 'left' | 'right';
  variant?: ButtonVariant;
  size?: ButtonSize;
  fullWidth?: boolean;
  className?: string;
  children: ReactNode;
} & ButtonHTMLAttributes<HTMLButtonElement>;

const VARIANT_CLASS: Record<ButtonVariant, string> = {
  primary: 'btn-primary',
  secondary: 'btn-secondary',
  'outline-primary': 'btn-outline-primary',
  'outline-secondary': 'btn-outline-secondary',
  light: 'btn-light',
  link: 'btn-link'
};

const SIZE_CLASS: Record<ButtonSize, string> = {
  md: '',
  sm: 'btn-sm'
};

const Button = ({
  icon,
  iconPosition = 'left',
  variant = 'primary',
  size = 'md',
  fullWidth,
  className,
  children,
  type = 'button',
  ...rest
}: TablerButtonProps) => {
  const classes = [
    'btn',
    VARIANT_CLASS[variant],
    'd-inline-flex',
    'align-items-center',
    'gap-2',
    'is-focusable'
  ];

  const sizeClass = SIZE_CLASS[size];
  if (sizeClass) classes.push(sizeClass);

  if (fullWidth) classes.push('w-100');
  if (className) classes.push(className);

  return (
    <button type={type} className={classes.join(' ').trim()} {...rest}>
      {iconPosition === 'left' && icon}
      {children}
      {iconPosition === 'right' && icon}
    </button>
  );
};

export default Button;
