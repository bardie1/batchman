/**
 * Interface for a batcher that can be shutdown
 * @interface IShutdown
 * @method shutdown - A method that will shutdown the batcher.
 */
export interface IShutdown {
  shutdown: () => Promise<void>;
}
