import { Logger } from '../lib/logger';
import { IJobSubmitter, IShutdown } from '../types';

export class MicroBatcher<T, R> extends Logger implements IJobSubmitter<T, R>, IShutdown {
  submitJob = jest.fn();
  shutdown = jest.fn().mockResolvedValue(void 0);
  log = jest.fn();
  debugLog = jest.fn();
}
