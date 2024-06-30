import { Job } from './job';

export interface IGetJob<T> {
  getJob: (key: string, id: string) => Job<T> | null;
}
