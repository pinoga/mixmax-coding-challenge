export interface LogMessage {
  message: string;
  meta: Record<string, unknown>;
}

enum LogLevel {
  INFO,
  ERROR,
}

export class Logger {
  private static _logger: Logger;
  public static instance(): Logger {
    if (!this._logger) {
      this._logger = new Logger();
    }
    return this._logger;
  }

  private log(level: LogLevel, msg: LogMessage): void {
    const msgString = JSON.stringify(msg);
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
