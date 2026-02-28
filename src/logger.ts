/**
 * Structured logger with context fields and child logger support.
 *
 * Output format:
 *   [2024-02-11T10:00:00.000Z] [INFO] [node=abc12345] [task=def67890] message text
 */

export type LogLevel = "debug" | "info" | "warn" | "error";

const LOG_LEVEL_ORDER: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

export interface LoggerOptions {
  /** Minimum log level to output. Defaults to "info". */
  level?: LogLevel;
  /** Context fields included in every log line. */
  fields?: Record<string, string>;
}

export class Logger {
  private level: number;
  private fields: Record<string, string>;

  constructor(opts?: LoggerOptions) {
    this.level = LOG_LEVEL_ORDER[opts?.level ?? "info"];
    this.fields = opts?.fields ?? {};
  }

  /**
   * Create a child logger that inherits this logger's level and fields,
   * with additional context fields merged in.
   */
  child(extraFields: Record<string, string>): Logger {
    const child = new Logger();
    child.level = this.level;
    child.fields = { ...this.fields, ...extraFields };
    return child;
  }

  debug(message: string, extra?: Record<string, string>): void {
    this.log("debug", message, extra);
  }

  info(message: string, extra?: Record<string, string>): void {
    this.log("info", message, extra);
  }

  warn(message: string, extra?: Record<string, string>): void {
    this.log("warn", message, extra);
  }

  error(message: string, extra?: Record<string, string>): void {
    this.log("error", message, extra);
  }

  private log(
    level: LogLevel,
    message: string,
    extra?: Record<string, string>,
  ): void {
    if (LOG_LEVEL_ORDER[level] < this.level) return;

    const timestamp = new Date().toISOString();
    const levelTag = level.toUpperCase();

    const allFields = extra
      ? { ...this.fields, ...extra }
      : this.fields;

    const fieldParts = Object.entries(allFields)
      .map(([k, v]) => `[${k}=${v}]`)
      .join(" ");

    const line = fieldParts
      ? `[${timestamp}] [${levelTag}] ${fieldParts} ${message}`
      : `[${timestamp}] [${levelTag}] ${message}`;

    if (level === "warn" || level === "error") {
      console.error(line);
    } else {
      console.log(line);
    }
  }
}
