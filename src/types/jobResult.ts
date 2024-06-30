import { Job } from './job';

/**
 * A JobResult is the result of processing a job.
 * @interface JobResult
 * @template T - The type of the job payload.
 * @template R - The type of the job result payload.
 * @extends {Job<T>}
 * @property {R} result - The result of the job.
 * @property {Date} completedAt - The date and time that the job was completed.
 */
export interface JobResult<T, R> extends Job<T> {
  result: R | null;
  completedAt?: Date;
}

/**
 * JobResultCallback is a function that is called when a job is processed.
 * @typedef JobResultCallback
 * @template T - The type of the job payload.
 * @template R - The type of the job result payload.
 * @param {JobResult<R>} result - The result of the job.
 * @returns {void}
 */
export type JobResultCallback<T, R> =
  | ((result: JobResult<T, R>) => Promise<void>)
  | ((result: JobResult<T, R>) => void);

/**
 * Asserts that the object is a JobResult
 * @param {JobResult<T,R>} jobResult
 * @returns {boolean} true if the object is a JobResult
 */
export function isJobResult<T, R>(jobResult: JobResult<T, R>): jobResult is JobResult<T, R> {
  return (
    'id' in jobResult &&
    'key' in jobResult &&
    'payload' in jobResult &&
    'status' in jobResult &&
    'submittedAt' in jobResult &&
    'result' in jobResult &&
    'errorReason' in jobResult &&
    jobResult.id !== undefined &&
    jobResult.key !== undefined &&
    jobResult.submittedAt instanceof Date &&
    ['submitted', 'processing', 'completed', 'failed'].includes(jobResult.status)
  );
}
