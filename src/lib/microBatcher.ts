import { Mutex } from 'async-mutex';
import {
  BatchProcessor,
  Job,
  JobErrorReason,
  JobResult,
  JobResultCallback,
  JobSubmission,
  isJobResult,
} from '../types';
import { BatchmanConfig, MicroBatcherBaseConfig } from '../types/config';
import { IShutdown } from '../types/shutdown';
import { IJobSubmitter } from '../types/submiter';
import { Logger } from './logger';
import { IGetJob } from '../types/getJob';
import { DEFAULT_DEBUG, DEFAULT_FREQUENCY, DEFAULT_MAX_BATCH_SIZE } from './batchman';

/**
 * MicroBatcher is a class that manages jobs and initiates processing of batches.
 * @class
 * @template T - The type of the job payload.
 * @template R - The type of the job result payload
 * @property {MicroBatcherBaseConfig} config - The configuration object for the microBatcher instance.
 * @property {BatchProcessor<T, R>} batchProcessor - The batch processor instance that will process the batch.
 * @property {Job<T>[]} jobs - The array of jobs that are currently in the batch.
 * @property {Map<Job<T>['id'], JobResultCallback<R>>} results - A map of job ids to their respective result callbacks.
 * @property {NodeJS.Timeout | null} timeout - The timeout object that will trigger the processing of the batch.
 * @property {boolean} shutdownFlag - A flag that indicates if the microBatcher instance is shutting down.
 * @property {(value: void | PromiseLike<void>) => void} shutdownResolve - A function that will resolve the shutdown promise.
 * @property {Mutex} mutex - A mutex object that will be used to prevent race conditions to jobs and results.
 * @property {boolean} isCheckingBatch - A flag that indicates if the batch is currently being checked for processing.
 */
export class MicroBatcher<T, R> extends Logger implements IJobSubmitter<T, R>, IShutdown, IGetJob<T> {
  /**
   * The configuration for the MicroBatcher instance.
   * @private
   * @type {MicroBatcherBaseConfig}
   * @see MicroBatcherBaseConfig
   * @memberof MicroBatcher
   * @readonly
   */
  private readonly config: MicroBatcherBaseConfig;

  /**
   * The batch processor instance that will process the batch.
   * @private
   * @type {BatchProcessor<T, R>}
   * @memberof MicroBatcher
   * @readonly
   * @see BatchProcessor
   */
  private readonly batchProcessor: BatchProcessor<T, R>;

  /**
   * The array of jobs that are currently in the batch.
   * @private
   * @type {Job<T>[]}
   * @see Job
   * @memberof MicroBatcher
   * @readonly
   */
  private readonly jobs: Job<T>[];

  /**
   * A map of job ids to their respective result callbacks. This is used to hold the callbacks to be called after the job is processed.
   * @private
   * @type {Map<Job<T>['id'], JobResultCallback<R>>}
   * @memberof MicroBatcher
   * @readonly
   * @see JobResultCallback
   * @see Job
   */
  private readonly results: Map<Job<T>['id'], JobResultCallback<T, R>>;

  /**
   * A map of job ids to their respective job objects. This is used to hold the jobs that are currently being processed.
   * @private
   * @type {Map<Job<T>['id'], Job<T>>}
   * @memberof MicroBatcher
   * @readonly
   * @see Job
   */
  private readonly processingJobs: Map<string, Job<T>>;

  /**
   * A mutex object that will be used to prevent shared resources from being accessed concurrently.
   * @private
   * @type {Mutex}
   * @memberof MicroBatcher
   * @readonly
   */
  private readonly mutex: Mutex = new Mutex();

  /**
   * The timeout object that will trigger the processing of the batch after the frequency has elapsed.
   * @private
   * @type {NodeJS.Timeout | null}
   * @memberof MicroBatcher
   */
  private timeout: NodeJS.Timeout | null;

  /**
   * A flag that indicates if the microBatcher instance is shutting down.
   * @private
   * @type {boolean}
   * @memberof MicroBatcher
   */
  private shutdownFlag: boolean;

  /**
   * A function that will resolve the shutdown promise when all jobs have been processed.
   * @private
   * @type {(value: void | PromiseLike<void>) => void}
   * @memberof MicroBatcher
   */
  private shutdownResolve: ((value: void | PromiseLike<void>) => void) | null;

  /**
   * A flag that indicates if the batch is currently being checked for processing. This is used to prevent multiple checks from happening concurrently.
   * @private
   * @type {boolean}
   * @memberof MicroBatcher
   */
  private isCheckingBatch: boolean = false;

  /**
   * Constructor for the MicroBatcher class.
   * @constructor
   * @param {MicroBatcherBaseConfig} config
   * @param {MicroBatcherBaseConfig} batchProcessor
   * @returns {MicroBatcher<T, R>}
   */
  constructor(config: MicroBatcherBaseConfig, batchProcessor: BatchProcessor<T, R>) {
    super(config.debug);
    this.mutex = new Mutex();
    MicroBatcher.validateConfig<T, R>(config, batchProcessor);

    this.config = config;
    this.jobs = [];
    this.results = new Map();
    this.processingJobs = new Map();
    this.timeout = null;
    this.batchProcessor = batchProcessor;
    this.shutdownFlag = false;
    this.shutdownResolve = null;
  }

  /**
   * Method to validate the configuration object for the microBatcher instance.
   * @param {MicroBatcherBaseConfig} config The configuration object for the microBatcher instance to validate
   * @param {BatchProcessor<T,R>} batchProcessor The batch processor instance that will process the batch
   */
  static validateConfig<T, R>(config: MicroBatcherBaseConfig, batchProcessor: BatchProcessor<T, R>) {
    if (config.frequency <= 0) throw new Error(`Frequency must be greater than 0`);
    if (config.maxBatchSize <= 0) throw new Error(`Max batch size must be greater than 0`);
    if (typeof batchProcessor.processBatch !== 'function')
      throw new Error(`Batch processor must have a processBatch function`);
  }

  /**
   * Converts a BatchmanConfig to a MicroBatcherBaseConfig.
   * @static
   * @method
   * @param batchmanConfig
   * @returns {MicroBatcherBaseConfig} The converted config as MicroBatcherBaseConfig
   */
  static MicroBatcherConfigFromBatchmanConfig(batchmanConfig: BatchmanConfig): MicroBatcherBaseConfig {
    return {
      frequency: batchmanConfig.frequency ?? DEFAULT_FREQUENCY,
      maxBatchSize: batchmanConfig.maxBatchSize ?? DEFAULT_MAX_BATCH_SIZE,
      debug: batchmanConfig.debug ?? DEFAULT_DEBUG,
    };
  }

  /**
   * Submits a job to the MicroBatcher instance.
   * @method
   * @public
   * @async
   * @param {JobSubmission<T>} job - The job to submit.
   * @param {JobResultCallback<R>} callback - The callback function to call when the job is processed.
   * @returns {Job} Job.
   */
  public async submitJob(jobSubmission: JobSubmission<T>, callback: JobResultCallback<T, R>): Promise<Job<T>> {
    /**
     * We will use the async-mutex library to ensure that we do not access the jobs and results arrays concurrently.
     * This is important in this method especially with regards to checking for duplicates.
     */
    return await this.mutex.runExclusive(async () => {
      const job: Job<T> = {
        ...jobSubmission,
        status: 'submitted',
        submittedAt: new Date(),
        errorReason: null,
      };

      // If the batcher is shutting down, we will not accept any new jobs.
      if (this.shutdownFlag) {
        this.log(`Cannot submit job ${jobSubmission.id} as batcher is shutting down`);
        job.status = 'failed';
        job.errorReason = JobErrorReason.SHUTDOWN_BATCH;
        job.errorMessage = 'Batcher is shutting down';
        return job;
      }

      // If the job is already in the batch, we will not accept it.
      if (this.jobs.some((jobInBatch) => jobInBatch.id === jobSubmission.id)) {
        this.debugLog(`Job with id ${jobSubmission.id} already in batch, ignoring`);
        job.status = 'failed';
        job.errorReason = JobErrorReason.DUPLICATE;
        job.errorMessage = 'Job already in batch';
        return job;
      }
      this.debugLog(`Submitting job ${jobSubmission.id}`);
      this.debugLog(`Full job payload: ${JSON.stringify(jobSubmission)}`);

      this.jobs.push(job);
      this.results.set(job.id, callback);

      /**
       * We will only check the batch if we are not already checking the batch.
       * This is to prevent unnessary checking that can arise when multiple jobs are submitted concurrently.
       */
      if (!this.isCheckingBatch) {
        this.isCheckingBatch = true;
        this.checkBatch();
      }

      return job;
    });
  }

  /**
   * Checks if the batch is full and initiates processing if it is. Otherwise sets a timeout for processing.
   * @method
   * @private
   * @returns {void}
   */
  private async checkBatch() {
    /**
     * Using mutex here is important to ensure the correct logic is executed when checking the batch.
     */
    return await this.mutex.runExclusive(async () => {
      this.debugLog(`Checking batch with ${this.jobs.length} jobs`);

      if (this.jobs.length >= this.config.maxBatchSize) {
        this.debugLog(`Batch is full, processing`);
        this.processBatch();
      } else if (!this.timeout) {
        this.debugLog(`Setting timeout for batch`);
        this.timeout = setTimeout(() => {
          this.processBatch();
        }, this.config.frequency);
      }

      // Release this flag so we can check the batch again.
      this.isCheckingBatch = false;
    });
  }

  /**
   * Asyncronously processes the batch by calling the batch processor and then calling the result callbacks.
   * @async
   * @method
   * @private
   * @returns {Promise<void>}
   */
  private async processBatch() {
    // We will use the async-mutex library to ensure that we do not access the jobs and results arrays concurrently.
    return await this.mutex.runExclusive(async () => {
      try {
        this.debugLog(`Processing batch with ${this.jobs.length} jobs`);
        clearTimeout(this.timeout as NodeJS.Timeout);

        const batch = this.jobs.splice(0, this.config.maxBatchSize).map((job) => {
          job.status = 'processing';
          this.processingJobs.set(job.id, job);
          return job;
        });

        this.debugLog(`Full batch payload: ${JSON.stringify(batch)}`);

        let results: JobResult<T, R>[] = [];

        /**
         * We don't know if the batch processor will throw an error so we will wrap it in a try catch block.
         * And then mark all the jobs in the batch as failed if an error occurs.
         */
        try {
          // FUTURE IMPROVEMENT: Pass a batch id to the batch processor to help with debugging and act as traceId.
          results = await this.batchProcessor.processBatch(batch);
          this.debugLog(`Received ${results.length} results. Full results payload: ${JSON.stringify(results)}`);
        } catch (error) {
          this.debugLog(`Error in batchProcessor: ${(error as Error).message}`);
          results = batch.map((job) => {
            return {
              ...job,
              status: 'failed',
              result: null,
              errorReason: JobErrorReason.GENERAL_ERROR,
              errorMessage: (error as Error).message,
              completedAt: new Date(),
            };
          });
        }

        // We don't trust the batch processor so we must validate the results are in face an array before continuing.
        try {
          if (!Array.isArray(results)) throw new Error(`Batch processor must return an array of results`);
        } catch (error) {
          const errorMessage = `Error validating results: ${(error as Error).message}`;
          this.debugLog(errorMessage);
          results = batch.map((job) => {
            return {
              ...job,
              status: 'failed',
              result: null,
              errorReason: JobErrorReason.GENERAL_ERROR,
              errorMessage: (error as Error).message,
              completedAt: new Date(),
            };
          });
        }

        /**
         * We must do robust checking on the results as the batch processor is a dependency that we cannot trust.
         * We must ensure that the results are an array,
         * We also don't know if the batch processor will do unpredicatble things like
         *  - Only return the successful jobs
         *  - Have flawed logic that returns a different object interface than expected
         *
         * So we need to check each result to ensure that it is a valid JobResult.
         *
         * We must also make sure the returned value adhear to the JobResult type.
         */
        batch.forEach((job) => {
          if (!results.some((result) => result.id === job.id && result.key === job.key)) {
            results.push({
              ...job,
              status: 'failed',
              result: null,
              errorReason: JobErrorReason.GENERAL_ERROR,
              errorMessage: `No result returned for job`,
              completedAt: new Date(),
            });

            this.debugLog(`No result returned for job ${job.id}`);
          }
        });

        for (let result of results) {
          // We find it's associated job so we can return the result in the correct structure.
          const associatedJob = batch.find((job) => job.id === result.id && job.key === result.key);
          if (!associatedJob) {
            this.log(`No associated job found for result: ${JSON.stringify(result)}`);
            continue;
          }

          if (!isJobResult(result)) {
            this.debugLog(`Result does not adhere to the JobResult type: ${JSON.stringify(result)}`);
            result = {
              ...associatedJob,
              result: null,
              status: 'failed',
              errorReason: JobErrorReason.GENERAL_ERROR,
              errorMessage: `Batch processor must return results that adhere to the JobResult type`,
            };
          }

          if (!result.completedAt) result.completedAt = new Date(); // Incase the batch processor does not set the completedAt field, we will set it here.
          if (result.status === 'processing') result.status = 'completed'; // Incase the batch processor does not update the status field, we will set it here.

          this.processingJobs.delete(result.id);

          const callback = this.results.get(result.id);
          // We dont know if the callback will throw an error so we will wrap it in a try catch block.
          // We still need to process it becuase it was in fact processed
          if (callback) {
            try {
              const res = callback(result);
              if (res instanceof Promise) await res;
              this.debugLog(`Callback called for job ${result.id}`);
            } catch (error) {
              this.log(`Callback Error: ${(error as Error).message}`);
            } finally {
              this.results.delete(result.id);
            }
          }
        }

        /**
         * If the shutdown flag is set and there are no jobs in the queue, we will shutdown the microBatcher instance
         * by using the shutdownResolve function we initially set when invoking shutdown().
         */
        if (this.shutdownFlag && this.jobs.length === 0) {
          this.debugLog(`Shutting down`);
          if (this.shutdownResolve) this.shutdownResolve();
        }

        if (this.jobs.length > 0) {
          /**
           * If there are still jobs in the queue, we will check the batch again.
           * If the remaining jobs exceed the batch size it's important to not that we must set a timeout to ensure that the frequency is respected.
           * If we don't set the timeout the jobs will be processed immediately and the frequency will not be respected.
           * NOTE: This is a design decision that can be changed if needed.
           */
          this.debugLog(`Jobs remaining (${this.jobs.length}) in queue, checking batch`);
          if (this.jobs.length >= this.config.maxBatchSize) {
            setTimeout(() => {
              this.checkBatch();
            }, this.config.frequency);
          } else {
            this.checkBatch();
          }
        }
      } catch (error) {
        this.log(`Error processing batch: ${(error as Error).message}`);
      }
    });
  }

  /**
   * Gets a job by id and key. Useful to check on a jobs current status.
   * We use the key because the key determines the type and the id could be duplicated across different keys.
   * @public
   * @method
   * @param {string} key The key related to the job
   * @param {string} id The id of the job to get.
   * @returns a job or null if the job is not found.
   */
  public getJob(key: string, id: string): Job<T> | null {
    const job = this.jobs.find((job) => job.id === id && job.key === key);
    if (job) return job;

    const processingJob = this.processingJobs.get(id);
    return processingJob || null;
  }

  /**
   * Asyncronously shuts down the microbatcher instance and resolves the shutdown promise when all jobs have been processed.
   * @async
   * @method
   * @public
   * @returns {Promise<void>}
   */
  public async shutdown() {
    this.debugLog(`Shutting down with ${this.jobs.length} jobs`);
    this.shutdownFlag = true;
    return new Promise<void>((resolve) => {
      if (this.jobs.length === 0) {
        this.debugLog(`No jobs in queue, shutting down immediately`);
        resolve();
      } else {
        this.debugLog(`Jobs in queue, waiting for processing to complete`);

        this.shutdownResolve = resolve; // We set this to a class variable so that we can resolve the promise when all jobs have been processed.

        if (this.timeout) {
          clearTimeout(this.timeout);
          this.timeout = null;
        }
        this.processBatch();
      }
    });
  }
}
