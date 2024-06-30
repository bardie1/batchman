/**
 * Class that provides logging functionality.
 * This is a representation of a logger in a production environment this would be a lot more verbose. Potentially using third party libraries.
 * @class Logger
 */
export class Logger {
  debug: boolean;
  constructor(debug: boolean) {
    this.debug = debug;
  }

  protected log(message: string): void {
    console.log(message);
  }

  protected debugLog(message: string): void {
    if (this.debug) this.log(message);
  }
}
