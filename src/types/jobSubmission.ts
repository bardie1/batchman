/**
 * Represents a job submission.
 * @interface JobSubmission
 * @template T - The type of the job payload.
 * @property {string} id - The unique identifier for the job.
 * @property {string} key - The key for the job.
 * @property {T} payload - The payload for the job.
 */
export interface JobSubmission<T> {
  id: string;
  key: string;
  payload: T;
}
