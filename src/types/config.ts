/**
 * @file This file contains the configuration types for the micro-batcher
 */

/**
 * The base configuration for the microBatcher
 * @interface MicroBatcherBaseConfig
 * @property {number} frequency - The frequency in milliseconds that the microBatcher should process the batch.
 * @property {number} maxBatchSize - The maximum number of jobs that should be processed in a single batch.
 * @property {boolean} debug - A flag to enable debug logging.
 */
export interface MicroBatcherBaseConfig {
  frequency: number;
  maxBatchSize: number;
  debug: boolean;
}

/**
 * The configuration for the microBatcher for a specific key. This configuration will override the default configuration.
 * @interface BatchmanKeyOverrideConfig
 * @extends {Partial<MicroBatcherBaseConfig>}
 * @property {string} key - The key that this configuration should be applied to. Cannot be 'default'.
 */
export interface BatchmanKeyOverrideConfig extends Partial<MicroBatcherBaseConfig> {
  key: Exclude<string, 'default'>;
}

/**
 * The configuration for the Batchman instance.
 * @interface BatchmanConfig
 * @extends {MicroBatcherBaseConfig}
 * @property {boolean} useInstancePerKey - A flag to determine if the Batchman should use a different MicroBatcher instance for each key.
 * @property {BatchmanKeyOverrideConfig[]} keyConfigOverrides - An array of key configurations that will override the default configuration.
 */
export interface BatchmanConfig extends Partial<MicroBatcherBaseConfig> {
  useInstancePerKey?: boolean;
  keyConfigOverrides?: BatchmanKeyOverrideConfig[];
}
