import { Job } from './job';
import { JobResult } from './jobResult';

/**
 * A BatchProcessor is any class or object that contains a processBatch method.
 * The processBatch method is used to process a batch of jobs.
 * The processBatch method should return a promise that resolves to an array of JobResults.
 * @see JobResult
 * @interface BatchProcessor
 * @template T - The type of the job payload.
 * @template R - The type of the job result payload.
 */
export interface BatchProcessor<T, R> {
  processBatch: (jobs: Job<T>[]) => Promise<JobResult<T, R>[]>;
}
