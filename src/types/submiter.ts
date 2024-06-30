import { Job } from './job';
import { JobResultCallback } from './jobResult';

/**
 * IJobSubmitter is an interface that represents a job submitter.
 * @interface IJobSubmitter
 * @template T - The type of the job payload.
 * @template R - The type of the job result payload.
 * @property {submitJob} submitJob - A method that submits a job.
 */
export interface IJobSubmitter<T, R> {
  submitJob: (job: Job<T>, callback: JobResultCallback<T, R>) => void;
}
