import { JobSubmission } from './jobSubmission';

export type JobStatus = 'submitted' | 'processing' | 'completed' | 'failed';
export enum JobErrorReason {
  TIMEOUT = 'timeout',
  GENERAL_ERROR = 'error',
  MAX_RETRY = 'max_retry',
  DUPLICATE = 'duplicate',
  SHUTDOWN_BATCH = 'shutdown_batch',
}

/**
 * A Job is a unit of work that can be processed.
 * @interface Job
 * @template T - The type of the job payload.
 * @extends {JobSubmission<T>}
 * @property {JobStatus} status - The status of the job.
 * @property {Date} submittedAt - The date and time that the job was submitted.
 * @property {JobErrorReason | null} errorReason - The reason that the job failed.
 * @property {string} errorMessage - The error message if the job failed.
 */
export interface Job<T> extends JobSubmission<T> {
  status: JobStatus;
  submittedAt: Date;
  errorReason: JobErrorReason | null;
  errorMessage?: string;
}
