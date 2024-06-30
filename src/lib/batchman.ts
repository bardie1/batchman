import { BatchProcessor, Job, JobResultCallback, JobSubmission } from '../types';
import { BatchmanConfig, BatchmanKeyOverrideConfig, MicroBatcherBaseConfig } from '../types/config';
import { Mutex } from 'async-mutex';
import { Logger } from './logger';
import { IJobSubmitter } from '../types/submiter';
import { IShutdown } from '../types/shutdown';
import { MicroBatcher } from './microBatcher';
import { IGetJob } from '../types/getJob';

export const DEFAULT_FREQUENCY = 5000;
export const DEFAULT_MAX_BATCH_SIZE = 10;
export const DEFAULT_DEBUG = false;
export const DEFAULT_USEINSTANCEPERKEY = true;
export const DEFAULT_KEYCONFIGOVERRIDES: BatchmanKeyOverrideConfig[] = [];

export type BatchProcessorFactory<T, R> = () => BatchProcessor<T, R>;

export type BatchProcessorConfig<T, R> = BatchProcessorFactory<T, R> | Map<string, BatchProcessorFactory<T, R>>;

/**
 * Batchman is a class that can manage jobs that require different processing.
 * @class
 * @template T - The type of the job payload.
 * @template R - The type of the job result payload.
 */
export class Batchman<T, R> extends Logger implements IJobSubmitter<T, R>, IShutdown, IGetJob<T> {
  /**
   * The configuration for the Batchman instance.
   * @private
   * @type {BatchmanConfig}
   * @memberof Batchman
   * @default
   * @readonly
   */
  private readonly config: Required<BatchmanConfig>;

  /**
   * The configuration for the BatchProcessor instance.
   * The BatchProcessor can be a single instance or a map of instances. Where the map key is the key of the job.
   * The Map of instances must have a default key. The map of instances is used when useInstancePerKey is true so that each key can be processed in it's own way.
   * @see BatchProcessor
   * @private
   * @type {BatchProcessorConfig<T,R>}
   * @memberof Batchman
   * @readonly
   */
  private readonly batchProcessorConfig: BatchProcessorConfig<T, R>;

  /**
   * A map of MicroBatcher instances. Each key in the map represents a different MicroBatcher instance.
   * They key is the key of the job and is used to determine which MicroBatcher instance to use.
   * @private
   * @type {Map<Job<T>['key'], MicroBatcher<T, R>>}
   * @memberof Batchman
   * @readonly
   */
  private readonly microBatcherInstances: Map<Job<T>['key'], MicroBatcher<T, R>>;

  /**
   * A mutex that is used to ensure that shared resources are not accessed concurrently.
   * @private
   * @type {Mutex}
   * @memberof Batchman
   * @readonly
   */
  private readonly mutex: Mutex;

  /**
   * The constructor for the Batchman class.
   * @param {BatchmanConfig} config
   * @param batchProcessorConfig
   * @returns {Batchman<T, R>} Instance of Batchman
   */
  constructor(config: BatchmanConfig, batchProcessorConfig: BatchProcessorConfig<T, R>) {
    super(config.debug ?? false);
    Batchman.validateConfig(config, batchProcessorConfig);
    Batchman.validateBatchProcessorConfig(batchProcessorConfig);
    this.config = {
      debug: config.debug ?? DEFAULT_DEBUG,
      frequency: config.frequency ?? DEFAULT_FREQUENCY,
      maxBatchSize: config.maxBatchSize ?? DEFAULT_MAX_BATCH_SIZE,
      useInstancePerKey: config.useInstancePerKey ?? DEFAULT_USEINSTANCEPERKEY,
      keyConfigOverrides: config.keyConfigOverrides ?? DEFAULT_KEYCONFIGOVERRIDES,
    };
    this.batchProcessorConfig = batchProcessorConfig;
    this.microBatcherInstances = new Map();
    this.mutex = new Mutex();

    const defaultMicroBatchConfig = MicroBatcher.MicroBatcherConfigFromBatchmanConfig(config);

    this.microBatcherInstances.set(
      'default',
      new MicroBatcher(
        defaultMicroBatchConfig,
        batchProcessorConfig instanceof Map ? batchProcessorConfig.get('default')!() : batchProcessorConfig(),
      ),
    );
  }

  /**
   * A static function that validates the configuration for a Batchman instance.
   * @static
   * @param config The config to validate
   * @param batchProcessorConfig The batch processor config to validate
   * @throws {Error} If the configuration is invalid
   * @returns {void}
   */
  static validateConfig<T, R>(config: BatchmanConfig, batchProcessorConfig: BatchProcessorConfig<T, R>) {
    if (config.frequency !== undefined && config.frequency <= 0) throw new Error(`Frequency must be greater than 0`);
    if (config.maxBatchSize !== undefined && config.maxBatchSize <= 0)
      throw new Error(`Max batch size must be greater than 0`);
    if (!config.useInstancePerKey && batchProcessorConfig instanceof Map)
      throw new Error(`Multiple Batch processors can only be used when useInstancePerKey is true`);
    if (!config.useInstancePerKey && config.keyConfigOverrides && config.keyConfigOverrides.length > 0)
      throw new Error(`Cannot use keyConfigOverrides when useInstancePerKey is false`);

    /**
     * Here we loop through the keyConfigOverrides and validate the configuration for each key.
     * We do this by calling the validateConfig function on the MicroBatcher class and for this we need the batchProcessor,
     * hence the retrieval of the batchProcessorFactory.
     */
    if (!config.keyConfigOverrides) return;
    config.keyConfigOverrides.forEach((keyConfig) => {
      let batchProcessorFactory: BatchProcessorFactory<T, R> | undefined;
      if (batchProcessorConfig instanceof Map) {
        if (batchProcessorConfig.has(keyConfig.key)) {
          batchProcessorFactory = batchProcessorConfig.get(keyConfig.key);
        } else {
          batchProcessorFactory = batchProcessorConfig.get('default');
        }
      } else if (typeof batchProcessorConfig === 'function') {
        batchProcessorFactory = batchProcessorConfig;
      }

      if (!batchProcessorFactory) throw new Error(`No batch processor found for key ${keyConfig.key}`);
      const microBatcherConfig = MicroBatcher.MicroBatcherConfigFromBatchmanConfig({
        ...config,
        ...keyConfig,
      });

      // This will throw an error if the configuration is invalid
      MicroBatcher.validateConfig(microBatcherConfig, batchProcessorFactory());
    });
  }

  /**
   * Validates the BatchProcessor configuration and throws and error if it is invalid.
   * @static
   * @param {BatchProcessorConfig<T, R>} batchProcessorConfig
   * @throws {Error} If the configuration is invalid
   * @returns {void}
   */
  static validateBatchProcessorConfig<T, R>(batchProcessorConfig: BatchProcessorConfig<T, R>) {
    // Remember we can pass in either a single BatchProcessor or a Map of BatchProcessors so we must check for both.
    if (typeof batchProcessorConfig === 'function') {
      if (typeof batchProcessorConfig().processBatch !== 'function')
        throw new Error(`Batch processor must have a processBatch function`);
    } else {
      if (!batchProcessorConfig.has('default')) throw new Error(`Batch processor must have a default processor`);
      for (const key of Array.from(batchProcessorConfig.keys())) {
        if (typeof batchProcessorConfig.get(key)!().processBatch !== 'function')
          throw new Error(`Batch processor must have a processBatch function`);
      }
    }
  }

  /**
   * Gets a batch processor for a given job key.
   * @param {string} key The key of the job
   * @returns {BatchProcessor<T, R>} A BatchProcessor instance for the given key
   */
  private getBatchProcessor(key: string): BatchProcessor<T, R> {
    if (this.batchProcessorConfig instanceof Map) {
      if (this.batchProcessorConfig.has(key)) {
        return this.batchProcessorConfig.get(key)!();
      } else {
        return this.batchProcessorConfig.get('default')!();
      }
    } else {
      return this.batchProcessorConfig();
    }
  }

  /**
   * Retrieves the configuration for a given key. For any properties that are not set in the keyConfigOverrides the default configuration is used.
   * @param key The key of a job
   * @returns {MicroBatcherBaseConfig} The Configuration for the given key
   */
  private getConfigForKey(key: string): MicroBatcherBaseConfig {
    const keyConfig = this.config.keyConfigOverrides.find((config) => config.key === key);
    return {
      debug: keyConfig?.debug ?? this.config.debug,
      frequency: keyConfig?.frequency ?? this.config.frequency,
      maxBatchSize: keyConfig?.maxBatchSize ?? this.config.maxBatchSize,
    };
  }

  /**
   * Async function that submits a job. It will check the configuration and job key to determine which MicroBatcher instance to use.
   * @param {JobSubmission<T,R>} job The job to submit
   * @param {JobResultCallback<T, R>} callback
   * @returns {Promise<Job<T>>} The job that was submitted
   */
  public async submitJob(job: JobSubmission<T>, callback: JobResultCallback<T, R>): Promise<Job<T>> {
    return this.mutex.runExclusive(async () => {
      /**
       * Remember that the configuration can be set to use a different MicroBatcher instance for each key.
       * If this is the case we need to check if the MicroBatcher instance exists and if not create it.
       * We then submit the job to the MicroBatcher instance.
       * Otherwise we submit the job to the default MicroBatcher instance.
       */
      if (this.config.useInstancePerKey) {
        if (!this.microBatcherInstances.has(job.key)) {
          const microBatcher = new MicroBatcher(this.getConfigForKey(job.key), this.getBatchProcessor(job.key));
          this.microBatcherInstances.set(job.key, microBatcher);
        }

        return this.microBatcherInstances.get(job.key)!.submitJob(job, callback);
      } else {
        return this.microBatcherInstances.get('default')!.submitJob(job, callback);
      }
    });
  }

  /**
   * Gets a job with the given id. It will check the configuration and job key to determine which MicroBatcher instance to use.
   * @param {string} key key related to the job
   * @param {string} id id of the job
   * @returns {Job<T> | null} The job with the given id or null if the job does not exist
   */
  public getJob(key: string, id: string): Job<T> | null {
    if (this.config.useInstancePerKey) {
      if (!this.microBatcherInstances.has(key)) {
        return null;
      }

      return this.microBatcherInstances.get(key)!.getJob(key, id);
    } else {
      return this.microBatcherInstances.get('default')!.getJob(key, id);
    }
  }

  /**
   * Async function that shuts down the Batchman instance. It will shut down all MicroBatcher instances.
   */
  public async shutdown() {
    this.debugLog(`Shutting down with ${this.microBatcherInstances.size} instances`);
    const promises = Array.from(this.microBatcherInstances.values()).map((batchman) => batchman.shutdown());
    await Promise.all(promises);
    this.debugLog(`All instances shut down`);
  }
}
