import { Component, type ErrorInfo, type ReactNode } from "react";
import { Button } from "@nanobpm/process-explorer/ui";
import {
  formatErrorSummary,
  normalizeError,
  shouldResetOnKeyChange,
} from "./routeErrorBoundaryLogic";

interface RouteErrorBoundaryProps {
  /**
   * Changes whenever the active route changes (the console passes
   * `location.pathname`). A change clears a held error, so navigating away from
   * a crashed view recovers the app without a full page reload.
   */
  resetKey: string;
  children: ReactNode;
}

interface RouteErrorBoundaryState {
  error: Error | null;
}

/**
 * Contains a routed view's render error to the main content area.
 *
 * Without a boundary, a single view throwing during render propagates to the
 * root and React unmounts the *entire* SPA — navigation rail included — leaving
 * a blank page with no way out but a manual reload (#457). The rail is rendered
 * *outside* this boundary, so it stays mounted and navigable; picking another
 * route changes `resetKey`, which clears the error and renders the new view
 * normally. The captured error is also logged (not swallowed) for debugging.
 */
export class RouteErrorBoundary extends Component<
  RouteErrorBoundaryProps,
  RouteErrorBoundaryState
> {
  state: RouteErrorBoundaryState = { error: null };

  static getDerivedStateFromError(thrown: unknown): RouteErrorBoundaryState {
    return { error: normalizeError(thrown) };
  }

  componentDidCatch(thrown: unknown, info: ErrorInfo): void {
    // Surface it rather than swallowing — the fallback is user-facing, the log
    // is for whoever is debugging the crash.
    console.error(
      "Console view crashed:",
      normalizeError(thrown),
      info.componentStack,
    );
  }

  componentDidUpdate(prev: RouteErrorBoundaryProps): void {
    if (
      shouldResetOnKeyChange(
        prev.resetKey,
        this.props.resetKey,
        this.state.error != null,
      )
    ) {
      this.setState({ error: null });
    }
  }

  private handleRetry = (): void => this.setState({ error: null });

  render(): ReactNode {
    const { error } = this.state;
    if (error) {
      return <RouteErrorFallback error={error} onRetry={this.handleRetry} />;
    }
    return this.props.children;
  }
}

/** The user-facing fallback shown in place of a crashed view. */
export function RouteErrorFallback({
  error,
  onRetry,
}: {
  error: Error;
  onRetry?: () => void;
}) {
  return (
    <div className="flex h-full items-center justify-center p-8" role="alert">
      <div className="max-w-lg rounded-lg border border-danger/30 bg-raised p-6 shadow-sm">
        <h1 className="text-lg font-semibold text-fg">
          This view hit an error
        </h1>
        <p className="mt-1 text-sm text-fg-muted">
          The rest of the console is still working — pick another item in the
          navigation to keep going, or try this view again.
        </p>
        <pre className="mt-3 max-h-40 overflow-auto rounded-md bg-hover p-3 text-xs text-fg-faint">
          {formatErrorSummary(error)}
        </pre>
        <div className="mt-4 flex gap-2">
          {onRetry && (
            <Button variant="primary" size="sm" onClick={onRetry}>
              Try again
            </Button>
          )}
          <Button size="sm" onClick={() => window.location.reload()}>
            Reload console
          </Button>
        </div>
      </div>
    </div>
  );
}
