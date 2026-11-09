import {
  type AnchorHTMLAttributes,
  type ButtonHTMLAttributes,
  type HTMLAttributes,
  type InputHTMLAttributes,
  type ReactNode,
  useCallback,
  useEffect,
  useId,
  useSyncExternalStore,
} from "react";
import { createPortal } from "react-dom";
import { MOBILE_MAX_WIDTH } from "@nanobpm/nano-app-schema";

// Shared UI primitives — the building blocks every view composes so the
// console reads as one product. All colours come from theme tokens
// (src/theme/tokens.css); never hardcode a palette colour in a view.

// ─────────────────────────────────────────────────────────────────────────
// Mobile-first primitives (unit A0)
//
// The single responsive breakpoint for the whole console is `MOBILE_MAX_WIDTH`
// imported from `@nanobpm/nano-app-schema` — never restate `640px` here. Every
// A-task consumes these primitives, so they are self-contained: a consumer at
// 375×812 gets no horizontal scroll from them.
// ─────────────────────────────────────────────────────────────────────────

/** The `matchMedia` query string for "narrow" (mobile) viewports, derived from
 * the one canonical breakpoint. Exported so callers and tests can bind to the
 * single source of truth rather than re-typing a width. */
export function mobileMediaQuery(maxWidth: string = MOBILE_MAX_WIDTH): string {
  return `(max-width: ${maxWidth})`;
}

const NARROW_QUERY = mobileMediaQuery();

function subscribeNarrow(onChange: () => void): () => void {
  if (typeof window === "undefined" || !window.matchMedia) return () => {};
  const mql = window.matchMedia(NARROW_QUERY);
  // Safari <14 only supports the deprecated addListener API.
  if (mql.addEventListener) {
    mql.addEventListener("change", onChange);
    return () => mql.removeEventListener("change", onChange);
  }
  mql.addListener(onChange);
  return () => mql.removeListener(onChange);
}

function getNarrowSnapshot(): boolean {
  if (typeof window === "undefined" || !window.matchMedia) return false;
  return window.matchMedia(NARROW_QUERY).matches;
}

/** `true` when the viewport is at or below the canonical mobile breakpoint
 * (`MOBILE_MAX_WIDTH`). Drives the responsive presentation of the SAME routes —
 * views branch on this rather than forking a mobile route tree. SSR-safe
 * (returns `false` on the server). */
export function useIsNarrow(): boolean {
  return useSyncExternalStore(subscribeNarrow, getNarrowSnapshot, () => false);
}

/** Responsive card grid used across the home rail, project/instance/app cards
 * and filter sheets. Columns collapse to one on a phone (intrinsic auto-fill —
 * no width breakpoint literal) so content never overflows horizontally. */
export function CardGrid({
  className = "",
  children,
  ...rest
}: { className?: string; children: ReactNode } & Omit<
  HTMLAttributes<HTMLDivElement>,
  "className" | "children"
>) {
  return (
    <div className={`nano-card-grid ${className}`} {...rest}>
      {children}
    </div>
  );
}

/** A tappable launcher/navigation card — the mobile counterpart of a rail item,
 * reused for the home rail and project/instance/app cards. Renders an `<a>`
 * when `href` is given, otherwise a `<button>`. Meets the 44px minimum touch
 * target (`.nano-touch`). The props are a discriminated union keyed on `href`:
 * anchor attributes are only permitted when `href` is set, button attributes
 * otherwise — so invalid props (e.g. `disabled` on a link, `href` on a button)
 * are rejected at compile time rather than spread onto the wrong element. */
type NavCardBaseProps = {
  label: ReactNode;
  description?: ReactNode;
  icon?: ReactNode;
  active?: boolean;
  className?: string;
};

type NavCardAnchorProps = NavCardBaseProps & { href: string } & Omit<
    AnchorHTMLAttributes<HTMLAnchorElement>,
    "className" | "href" | "children" | "dangerouslySetInnerHTML"
  >;

type NavCardButtonProps = NavCardBaseProps & { href?: undefined } & Omit<
    ButtonHTMLAttributes<HTMLButtonElement>,
    "className" | "href" | "type" | "children" | "dangerouslySetInnerHTML"
  >;

export function NavCard(props: NavCardAnchorProps | NavCardButtonProps) {
  const {
    label,
    description,
    icon,
    active = false,
    href,
    className = "",
    ...rest
  } = props;
  const classes = `nano-touch flex w-full items-center gap-3 rounded-xl border bg-raised p-4 text-left shadow-sm transition-colors outline-none focus-visible:ring-2 focus-visible:ring-accent/60 ${
    active
      ? "border-accent/60 bg-accent/10"
      : "border-edge hover:border-edge-strong hover:bg-hover"
  } ${className}`;
  const inner = (
    <>
      {icon && (
        <span className="flex shrink-0 items-center text-fg-muted" aria-hidden>
          {icon}
        </span>
      )}
      <span className="min-w-0 flex-1">
        <span className="block truncate text-sm font-medium text-fg">
          {label}
        </span>
        {description && (
          <span className="mt-0.5 block truncate text-xs text-fg-muted">
            {description}
          </span>
        )}
      </span>
    </>
  );
  if (href !== undefined) {
    return (
      <a
        href={href}
        aria-current={active ? "page" : undefined}
        className={classes}
        {...(rest as AnchorHTMLAttributes<HTMLAnchorElement>)}
      >
        {inner}
      </a>
    );
  }
  return (
    <button
      aria-current={active ? "page" : undefined}
      className={classes}
      {...(rest as ButtonHTMLAttributes<HTMLButtonElement>)}
      type="button"
    >
      {inner}
    </button>
  );
}

/** A bottom-anchored sheet for mobile — the home hamburger menu, filter panels
 * and card drill-ins all use it. Slides up from the bottom edge, clears the
 * home indicator (`env(safe-area-inset-bottom)`), traps nothing but closes on
 * Escape or backdrop tap. Renders `null` when closed. */
export function BottomSheet({
  open,
  onClose,
  title,
  children,
  className = "",
}: {
  open: boolean;
  onClose: () => void;
  title?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  const handleKey = useCallback(
    (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    },
    [onClose],
  );

  const titleId = useId();

  useEffect(() => {
    if (!open) return;
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [open, handleKey]);

  if (!open || typeof document === "undefined") return null;

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-end justify-center bg-black/50"
      onClick={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={title ? titleId : undefined}
        aria-label={title ? undefined : "Dialog"}
        className={`nano-safe-bottom flex max-h-[85vh] w-full flex-col overflow-hidden rounded-t-2xl border-t border-edge-strong bg-raised shadow-xl ${className}`}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex justify-center pt-2 pb-1">
          <span aria-hidden className="nano-sheet-grip" />
        </div>
        <div className="flex shrink-0 items-center justify-between gap-2 border-b border-edge px-4 pb-3">
          {title ? (
            <h2
              id={titleId}
              className="min-w-0 flex-1 truncate text-sm font-semibold text-fg"
            >
              {title}
            </h2>
          ) : (
            <span className="flex-1" />
          )}
          <button
            type="button"
            className="nano-touch -mr-2 rounded p-2 text-fg-muted hover:bg-hover hover:text-fg"
            aria-label="Close"
            onClick={onClose}
          >
            ✕
          </button>
        </div>
        <div className="min-h-0 flex-1 overflow-auto p-4">{children}</div>
      </div>
    </div>,
    document.body,
  );
}

/** Standard page chrome: title, one-line subtitle, optional right-side actions. */
export function PageHeader({
  title,
  subtitle,
  actions,
}: {
  title: ReactNode;
  subtitle?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <header className="mb-6 flex flex-wrap items-start justify-between gap-3">
      <div>
        <h1 className="text-xl font-semibold tracking-tight text-fg">
          {title}
        </h1>
        {subtitle && (
          <p className="mt-1 max-w-2xl text-sm text-fg-muted">{subtitle}</p>
        )}
      </div>
      {actions && (
        <div className="flex shrink-0 items-center gap-2">{actions}</div>
      )}
    </header>
  );
}

/** Uppercase section label above a group of cards/rows. */
export function SectionLabel({ children }: { children: ReactNode }) {
  return (
    <h2 className="mb-2 text-xs font-semibold uppercase tracking-wider text-fg-faint">
      {children}
    </h2>
  );
}

export function Card({
  className = "",
  children,
  ...rest
}: { className?: string; children: ReactNode } & Record<string, unknown>) {
  return (
    <div
      className={`rounded-xl border border-edge bg-raised shadow-sm ${className}`}
      {...rest}
    >
      {children}
    </div>
  );
}

type ButtonVariant = "primary" | "secondary" | "danger" | "ghost";
type ButtonSize = "sm" | "md";

const buttonVariants: Record<ButtonVariant, string> = {
  primary:
    "bg-accent text-on-accent hover:bg-accent-strong border border-transparent shadow-sm",
  secondary:
    "border border-edge-strong bg-raised text-fg hover:bg-hover shadow-sm",
  danger: "bg-danger/10 text-danger border border-danger/30 hover:bg-danger/20",
  ghost: "border border-transparent text-fg-muted hover:bg-hover hover:text-fg",
};

const buttonSizes: Record<ButtonSize, string> = {
  sm: "px-3 py-1.5 text-xs",
  md: "px-4 py-2 text-sm",
};

export function Button({
  variant = "secondary",
  size = "md",
  className = "",
  ...rest
}: {
  variant?: ButtonVariant;
  size?: ButtonSize;
} & ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      className={`inline-flex items-center justify-center gap-1.5 rounded-md font-medium transition-colors outline-none focus-visible:ring-2 focus-visible:ring-accent/60 disabled:cursor-not-allowed disabled:opacity-50 ${buttonVariants[variant]} ${buttonSizes[size]} ${className}`}
      {...rest}
    />
  );
}

/** A small spinning activity indicator for in-progress affordances. Inherits
 * the current text colour (`currentColor`) so it tints to match its context.
 * `aria-hidden` — pair it with visible text (e.g. "Updating…") for a11y. */
export function Spinner({ className = "" }: { className?: string }) {
  return (
    <svg
      className={`h-3.5 w-3.5 animate-spin ${className}`}
      viewBox="0 0 24 24"
      fill="none"
      aria-hidden="true"
    >
      <circle
        className="opacity-25"
        cx="12"
        cy="12"
        r="10"
        stroke="currentColor"
        strokeWidth="4"
      />
      <path
        className="opacity-90"
        fill="currentColor"
        d="M4 12a8 8 0 0 1 8-8V0C5.373 0 0 5.373 0 12h4z"
      />
    </svg>
  );
}

type BadgeTone = "neutral" | "accent" | "ok" | "warn" | "danger" | "info";

const badgeTones: Record<BadgeTone, string> = {
  neutral: "bg-hover text-fg-muted",
  accent: "bg-accent/10 text-accent-strong border border-accent/30",
  ok: "bg-ok/10 text-ok",
  warn: "bg-warn/10 text-warn",
  danger: "bg-danger/10 text-danger",
  info: "bg-info/10 text-info",
};

export function Badge({
  tone = "neutral",
  className = "",
  children,
}: {
  tone?: BadgeTone;
  className?: string;
  children: ReactNode;
}) {
  return (
    <span
      className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium ${badgeTones[tone]} ${className}`}
    >
      {children}
    </span>
  );
}

/** The console's shared centered dialog: a backdrop that closes on click, a
 * raised panel that stops propagation, and a title over caller-supplied
 * content (including any footer buttons). Every simple confirmation/edit
 * dialog composes this so the markup has one implementation — the panels'
 * richer dialogs (header close button, scrollable body, fixed footer) are a
 * different shape and keep their own shells. */
export function Modal({
  title,
  children,
  onClose,
}: {
  title: string;
  children: ReactNode;
  onClose: () => void;
}) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-lg rounded-lg border border-edge bg-raised p-5 shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <h2 className="mb-4 text-lg font-semibold text-fg">{title}</h2>
        {children}
      </div>
    </div>
  );
}

/** Shared text-input styling (also exported as a class string for selects /
 * textareas that can't use the component). */
export const inputClass =
  "rounded-md border border-edge bg-inset px-3 py-2 text-sm text-fg placeholder:text-fg-faint outline-none transition-colors focus:border-accent focus-visible:ring-2 focus-visible:ring-accent/40";

export function Input({
  className = "",
  ...rest
}: InputHTMLAttributes<HTMLInputElement>) {
  return <input className={`${inputClass} ${className}`} {...rest} />;
}

export function EmptyState({
  title,
  hint,
  action,
}: {
  title: ReactNode;
  hint?: ReactNode;
  action?: ReactNode;
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 rounded-xl border border-dashed border-edge-strong px-6 py-12 text-center">
      <div className="text-sm font-medium text-fg-muted">{title}</div>
      {hint && <div className="max-w-md text-xs text-fg-faint">{hint}</div>}
      {action && <div className="mt-2">{action}</div>}
    </div>
  );
}

/** Inline error line under a form/action. */
export function ErrorText({ children }: { children: ReactNode }) {
  return <div className="text-sm text-danger">{children}</div>;
}
