export interface LogMessage {
  message: string;
  meta: Record<string, unknown>;
}

enum LogLevel {
  ERROR,
  INFO,
}

export class Logger {
  private static _logger: Logger;
  public static instance(): Logger {
    if (!this._logger) {
      this._logger = new Logger();
    }
    return this._logger;
  }

  private static replacer(_key: string, value: unknown): unknown {
    if (value instanceof Error) {
      return { name: value.name, message: value.message, stack: value.stack };
    }
    return value;
  }

  private log(level: LogLevel, msg: LogMessage): void {
    const msgString = JSON.stringify(msg, Logger.replacer);
    switch (level) {
      case LogLevel.ERROR:
        return console.error(msgString);
      case LogLevel.INFO:
        return console.log(msgString);
    }
  }

  public info(msg: LogMessage): void {
    return this.log(LogLevel.INFO, msg);
  }

  public error(msg: LogMessage): void {
    return this.log(LogLevel.ERROR, msg);
  }
}
