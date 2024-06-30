import { MicroBatcher } from '../lib';
import { Logger } from '../lib/logger';
import { BatchProcessor, Job, JobErrorReason, JobResult, JobStatus, JobSubmission } from '../types';
import { wait } from '../utils/wait';

class MockBatchProcessor implements BatchProcessor<number, string> {
  processBatch(jobs: Job<number>[]): Promise<JobResult<number, string>[]> {
    // Implement your mock batch processing logic here
    return Promise.resolve(
      jobs.map((job) => ({
        id: job.id,
        key: job.key,
        result: 'success',
        status: 'completed',
        errorReason: null,
        payload: job.payload,
        submittedAt: job.submittedAt,
        completedAt: new Date(),
      })),
    );
  }
}

const JOB_SUB_1: JobSubmission<number> = { id: '1', key: 'key1', payload: 10 };
const JOB_SUB_2: JobSubmission<number> = { id: '2', key: 'key1', payload: 20 };
const JOB_SUB_3: JobSubmission<number> = { id: '3', key: 'key1', payload: 30 };
const JOB_SUB_4: JobSubmission<number> = { id: '4', key: 'key1', payload: 40 };
const JOB_SUB_5: JobSubmission<number> = { id: '5', key: 'key1', payload: 50 };
const JOB_SUB_6: JobSubmission<number> = { id: '6', key: 'key1', payload: 60 };

const DEFAULT_SUCCESS_RESULT = {
  result: 'success',
  status: 'completed' as JobStatus,
  errorReason: null,
  submittedAt: expect.any(Date),
  completedAt: expect.any(Date),
};

const JOB_SUB_1_SUCCESS_RESULT: JobResult<number, string> = { ...JOB_SUB_1, ...DEFAULT_SUCCESS_RESULT };
const JOB_SUB_2_SUCCESS_RESULT: JobResult<number, string> = { ...JOB_SUB_2, ...DEFAULT_SUCCESS_RESULT };
const JOB_SUB_3_SUCCESS_RESULT: JobResult<number, string> = { ...JOB_SUB_3, ...DEFAULT_SUCCESS_RESULT };
const JOB_SUB_4_SUCCESS_RESULT: JobResult<number, string> = { ...JOB_SUB_4, ...DEFAULT_SUCCESS_RESULT };
const JOB_SUB_5_SUCCESS_RESULT: JobResult<number, string> = { ...JOB_SUB_5, ...DEFAULT_SUCCESS_RESULT };
const JOB_SUB_6_SUCCESS_RESULT: JobResult<number, string> = { ...JOB_SUB_6, ...DEFAULT_SUCCESS_RESULT };

const BATCH_SIZE = 3;
const FREQUENCY = 1000;
const FREQUENCY_BUFFER = 300; // Buffer to account for setTimeout inaccuracy
const PROCESSING_WAIT_TIME = FREQUENCY + FREQUENCY_BUFFER;

const DEFAULT_CONFIG = {
  maxBatchSize: BATCH_SIZE,
  frequency: FREQUENCY,
  debug: false,
};

describe('MicroBatcher', () => {
  let batcher: MicroBatcher<number, string>;
  let mockBatchProcessor: BatchProcessor<number, string>;

  beforeEach(() => {
    mockBatchProcessor = new MockBatchProcessor();
    batcher = new MicroBatcher(DEFAULT_CONFIG, mockBatchProcessor);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('Configuration Validation', () => {
    it('should throw error for invalid maxBatchSize', () => {
      expect(() => new MicroBatcher({ ...DEFAULT_CONFIG, maxBatchSize: 0 }, mockBatchProcessor)).toThrow(
        'Max batch size must be greater than 0',
      );
    });

    it('should throw error for invalid frequency', () => {
      expect(() => new MicroBatcher({ ...DEFAULT_CONFIG, frequency: 0 }, mockBatchProcessor)).toThrow(
        'Frequency must be greater than 0',
      );
    });

    it("should throw an error if processor doesn't implement processBatch", () => {
      const invalidProcessor = {};
      expect(() => new MicroBatcher(DEFAULT_CONFIG, invalidProcessor as any)).toThrow(
        'Batch processor must have a processBatch function',
      );
    });
  });

  describe('Job Submission and Processing', () => {
    it('should submit a job and call the callback when processed', async () => {
      const callback = jest.fn();

      const job = await batcher.submitJob(JOB_SUB_1, callback);
      expect(job).toEqual({
        ...JOB_SUB_1,
        status: 'submitted',
        errorReason: null,
        submittedAt: expect.any(Date),
      });

      expect(callback).toHaveBeenCalledTimes(0);

      await wait(PROCESSING_WAIT_TIME); // Wait for batch processing

      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith(JOB_SUB_1_SUCCESS_RESULT);
    });

    it('should submit a job and call the ASYNC callback when processed', async () => {
      let called = false;
      const callback = async (result: JobResult<number, string>) => {
        await wait(500);
        called = true;
      };

      const job = await batcher.submitJob(JOB_SUB_1, callback);
      expect(job).toEqual({
        ...JOB_SUB_1,
        status: 'submitted',
        errorReason: null,
        submittedAt: expect.any(Date),
      });

      await wait(PROCESSING_WAIT_TIME + 600); // Wait for batch processing and account for async callback

      expect(called).toBe(true);
    });

    it('should ignore duplicate job submissions', async () => {
      const callback = jest.fn();

      await batcher.submitJob(JOB_SUB_1, callback);
      await batcher.submitJob(JOB_SUB_1, callback);

      await wait(1500); // Wait for batch processing

      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith(JOB_SUB_1_SUCCESS_RESULT);
    });

    it('should be able to retrieve job', async () => {
      const callback = jest.fn();
      const batchProcessor = {
        processBatch: async () => {
          await wait(500);
          return Promise.resolve([JOB_SUB_1_SUCCESS_RESULT]);
        },
      };

      const newBatcher = new MicroBatcher(DEFAULT_CONFIG, batchProcessor);

      const job = await newBatcher.submitJob(JOB_SUB_1, callback);
      const retrievedJob = newBatcher.getJob(JOB_SUB_1.key, JOB_SUB_1.id);

      expect(retrievedJob).toEqual(job);

      await wait(1100); // Wait for batch processing
      const retrievedJobDuring = newBatcher.getJob(JOB_SUB_1.key, JOB_SUB_1.id);
      expect(retrievedJobDuring).toEqual({
        ...job,
        status: 'processing',
      });

      await wait(500); // Wait for batch processing
      const retrievedJobAfter = newBatcher.getJob(JOB_SUB_1.key, JOB_SUB_1.id);
      expect(retrievedJobAfter).toBeNull(); //Because it has been processed and is no longer in the batcher
    });

    it('should process multiple jobs in a batch', async () => {
      const callback1 = jest.fn();
      const callback2 = jest.fn();
      const callback3 = jest.fn();

      await batcher.submitJob(JOB_SUB_1, callback1);
      await batcher.submitJob(JOB_SUB_2, callback2);
      await batcher.submitJob(JOB_SUB_3, callback3);

      await new Promise((resolve) => setTimeout(resolve, 100)); // Wait for batch processing

      expect(callback1).toHaveBeenCalledTimes(1);
      expect(callback1).toHaveBeenCalledWith(JOB_SUB_1_SUCCESS_RESULT);
      expect(callback2).toHaveBeenCalledTimes(1);
      expect(callback2).toHaveBeenCalledWith(JOB_SUB_2_SUCCESS_RESULT);
      expect(callback3).toHaveBeenCalledTimes(1);
      expect(callback3).toHaveBeenCalledWith(JOB_SUB_3_SUCCESS_RESULT);
    });

    it('should process remaining jobs when shutting down', async () => {
      const callback1 = jest.fn();
      const callback2 = jest.fn();

      await batcher.submitJob(JOB_SUB_1, callback1);
      await batcher.submitJob(JOB_SUB_2, callback2);

      batcher.shutdown();

      await wait(PROCESSING_WAIT_TIME); // Wait for batch processing

      expect(callback1).toHaveBeenCalledTimes(1);
      expect(callback1).toHaveBeenCalledWith(JOB_SUB_1_SUCCESS_RESULT);
      expect(callback2).toHaveBeenCalledTimes(1);
      expect(callback2).toHaveBeenCalledWith(JOB_SUB_2_SUCCESS_RESULT);
    });

    it('should set completedAt if processor didnt return a completedAt', async () => {
      const callback = jest.fn();
      const batchProcessor = {
        processBatch: () =>
          Promise.resolve([
            {
              ...JOB_SUB_1_SUCCESS_RESULT,
              completedAt: undefined,
              submittedAt: new Date(),
            },
            {
              ...JOB_SUB_2_SUCCESS_RESULT,
              completedAt: undefined,
              submittedAt: new Date(),
            },
          ]),
      };

      const newBatcher = new MicroBatcher(DEFAULT_CONFIG, batchProcessor);
      await newBatcher.submitJob(JOB_SUB_1, callback);
      await newBatcher.submitJob(JOB_SUB_2, callback);

      await wait(PROCESSING_WAIT_TIME); // Wait for batch processing

      expect(callback).toHaveBeenCalledTimes(2);
      expect(callback).toHaveBeenNthCalledWith(1, JOB_SUB_1_SUCCESS_RESULT);
      expect(callback).toHaveBeenNthCalledWith(2, JOB_SUB_2_SUCCESS_RESULT);
    });

    it('should change status from processing to completed if processor didnt change it', async () => {
      const callback = jest.fn();
      const batchProcessor = {
        processBatch: () =>
          Promise.resolve([
            {
              ...JOB_SUB_1_SUCCESS_RESULT,
              status: 'processing' as JobStatus,
              completedAt: new Date(),
              submittedAt: new Date(),
            },
            {
              ...JOB_SUB_2_SUCCESS_RESULT,
              status: 'processing' as JobStatus,
              completedAt: new Date(),
              submittedAt: new Date(),
            },
          ]),
      };

      const newBatcher = new MicroBatcher(DEFAULT_CONFIG, batchProcessor);
      await newBatcher.submitJob(JOB_SUB_1, callback);
      await newBatcher.submitJob(JOB_SUB_2, callback);

      await wait(PROCESSING_WAIT_TIME); // Wait for batch processing

      expect(callback).toHaveBeenCalledTimes(2);
      expect(callback).toHaveBeenNthCalledWith(1, JOB_SUB_1_SUCCESS_RESULT);
      expect(callback).toHaveBeenNthCalledWith(2, JOB_SUB_2_SUCCESS_RESULT);
    });

    it('should throw an error when submitting a job after shutdown', async () => {
      const callback = jest.fn();
      const logSpy = jest.spyOn(Logger.prototype as any, 'log');
      batcher.shutdown();
      const result = await batcher.submitJob(JOB_SUB_1, callback);
      expect(result).toEqual({
        ...JOB_SUB_1,
        status: 'failed',
        errorReason: 'shutdown_batch',
        submittedAt: expect.any(Date),
        errorMessage: `Batcher is shutting down`,
      });
      expect(logSpy).toHaveBeenNthCalledWith(1, `Cannot submit job ${JOB_SUB_1.id} as batcher is shutting down`);
    });
  });

  describe('Debug Logging', () => {
    it('should call log and debugLog when debug is true', async () => {
      const callback = jest.fn();
      const newBatcher = new MicroBatcher({ ...DEFAULT_CONFIG, debug: true }, mockBatchProcessor);
      const logSpy = jest.spyOn(Logger.prototype as any, 'log');
      const debugLogSpy = jest.spyOn(Logger.prototype as any, 'debugLog');
      await newBatcher.submitJob(JOB_SUB_1, callback);
      await wait(PROCESSING_WAIT_TIME); // Wait for batch processing
      expect(logSpy).toHaveBeenCalled();
      expect(debugLogSpy).toHaveBeenCalled();
    });
  });

  describe('Error Handling', () => {
    it('should catch errors thrown in the callback', async () => {
      const callback = jest.fn(() => {
        throw new Error('Error in callback');
      });
      const logSpy = jest.spyOn(batcher as any, 'log');
      await batcher.submitJob(JOB_SUB_1, callback);
      await wait(PROCESSING_WAIT_TIME); // Wait for batch processing

      expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('Callback Error: Error in callback'));
    });

    it('should catch and throw errors when running processBatch', async () => {
      const callback = jest.fn();

      const logSpy = jest.spyOn(Logger.prototype as any, 'log');
      await batcher.submitJob(JOB_SUB_1, callback);

      (batcher as any).jobs = {};
      await wait(PROCESSING_WAIT_TIME); // Wait for batch processing

      expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('Error processing batch:'));
    });

    it('should handle error if the batch processor returns not an array', async () => {
      const callback = jest.fn();
      const batchProcessor = {
        processBatch: () =>
          Promise.resolve({
            ...JOB_SUB_1_SUCCESS_RESULT,
          }),
      };

      const logSpy = jest.spyOn(Logger.prototype as any, 'debugLog');

      //@ts-expect-error - Testing invalid return type
      const newBatcher = new MicroBatcher(DEFAULT_CONFIG, batchProcessor);
      await newBatcher.submitJob(JOB_SUB_1, callback);

      await wait(PROCESSING_WAIT_TIME); // Wait for batch processing
      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith({
        ...JOB_SUB_1,
        result: null,
        status: 'failed',
        errorReason: JobErrorReason.GENERAL_ERROR,
        errorMessage: 'Batch processor must return an array of results',
        submittedAt: expect.any(Date),
        completedAt: expect.any(Date),
      });
      expect(logSpy).toHaveBeenCalledWith(
        expect.stringContaining(`Error validating results: Batch processor must return an array of results`),
      );
    });

    it('should handle error if the batch processor returns array of different size to jobs provided', async () => {
      const callback = jest.fn();
      const batchProcessor = {
        processBatch: () =>
          Promise.resolve([
            {
              ...JOB_SUB_1_SUCCESS_RESULT,
              submittedAt: new Date(),
              completedAt: new Date(),
            },
            {
              ...JOB_SUB_2_SUCCESS_RESULT,
              submittedAt: new Date(),
              completedAt: new Date(),
            },
          ]),
      };

      const logSpy = jest.spyOn(Logger.prototype as any, 'log');

      const newBatcher = new MicroBatcher(DEFAULT_CONFIG, batchProcessor);
      await newBatcher.submitJob(JOB_SUB_1, callback);

      await wait(PROCESSING_WAIT_TIME); // Wait for batch processing
      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith(JOB_SUB_1_SUCCESS_RESULT);
      expect(logSpy).toHaveBeenCalledWith(expect.stringContaining(`No associated job found for result:`));
    });

    it('should handle error if the batch processor doesnt return a result for all the jobs it was provided', async () => {
      const callback = jest.fn();
      const batchProcessor = {
        processBatch: () =>
          Promise.resolve([
            {
              ...JOB_SUB_1_SUCCESS_RESULT,
              completedAt: new Date(),
              submittedAt: new Date(),
            },
            {
              ...JOB_SUB_3_SUCCESS_RESULT,
              completedAt: new Date(),
              submittedAt: new Date(),
            },
          ]),
      };

      const logSpy = jest.spyOn(Logger.prototype as any, 'debugLog');

      const newBatcher = new MicroBatcher(DEFAULT_CONFIG, batchProcessor);
      await newBatcher.submitJob(JOB_SUB_1, callback);
      await newBatcher.submitJob(JOB_SUB_2, callback);

      await wait(PROCESSING_WAIT_TIME); // Wait for batch processing
      expect(callback).toHaveBeenCalledTimes(2);
      expect(callback).toHaveBeenNthCalledWith(1, JOB_SUB_1_SUCCESS_RESULT);
      expect(callback).toHaveBeenNthCalledWith(2, {
        ...JOB_SUB_2,
        result: null,
        status: 'failed',
        errorReason: JobErrorReason.GENERAL_ERROR,
        errorMessage: 'No result returned for job',
        submittedAt: expect.any(Date),
        completedAt: expect.any(Date),
      });
      expect(logSpy).toHaveBeenCalledWith(expect.stringContaining(`No result returned for job ${JOB_SUB_2.id}`));
    });

    it('should handle error if the batch processor returns a result that does not adhere to the JobResult Interface', async () => {
      const callback = jest.fn();
      const batchProcessor = {
        processBatch: () => {
          const job1 = {
            ...JOB_SUB_1_SUCCESS_RESULT,
            completedAt: new Date(),
            submittedAt: new Date(),
          };

          //@ts-expect-error - Testing invalid return type
          delete job1.submittedAt;
          return Promise.resolve([
            job1,
            {
              ...JOB_SUB_2_SUCCESS_RESULT,
              completedAt: new Date(),
              submittedAt: new Date(),
            },
          ]);
        },
      };

      const newBatcher = new MicroBatcher(DEFAULT_CONFIG, batchProcessor);
      await newBatcher.submitJob(JOB_SUB_1, callback);
      await newBatcher.submitJob(JOB_SUB_2, callback);

      await wait(PROCESSING_WAIT_TIME); // Wait for batch processing

      expect(callback).toHaveBeenCalledTimes(2);
      expect(callback).toHaveBeenNthCalledWith(1, {
        ...JOB_SUB_1,
        result: null,
        status: 'failed',
        errorReason: JobErrorReason.GENERAL_ERROR,
        errorMessage: 'Batch processor must return results that adhere to the JobResult type',
        submittedAt: expect.any(Date),
        completedAt: expect.any(Date),
      });
      expect(callback).toHaveBeenNthCalledWith(2, JOB_SUB_2_SUCCESS_RESULT);
    });
  });

  describe('Simultaneous Job Submissions', () => {
    it('should ignore concurrent duplicate job submissions', async () => {
      const callback = jest.fn();
      const promises = [
        batcher.submitJob(JOB_SUB_1, callback),
        batcher.submitJob(JOB_SUB_1, callback),
        batcher.submitJob(JOB_SUB_1, callback),
      ];
      await Promise.all(promises);
      await wait(PROCESSING_WAIT_TIME); // Wait for batch processing
      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith(JOB_SUB_1_SUCCESS_RESULT);

      await batcher.shutdown();
    });

    it('should handle simultaneous job submissions correctly', async () => {
      const callbackHolder = {
        cb: async () => {
          return new Promise((resolve) => setTimeout(resolve, 10));
        },
      };

      const spy = jest.spyOn(callbackHolder, 'cb');
      const processBatchSpy = jest.spyOn(mockBatchProcessor, 'processBatch');
      const jobs: JobSubmission<any>[] = [JOB_SUB_1, JOB_SUB_2, JOB_SUB_3, JOB_SUB_4];

      const submissions = await Promise.all(jobs.map((job) => batcher.submitJob(job, callbackHolder.cb)));
      await wait(PROCESSING_WAIT_TIME * 2); // Wait for processing to complete

      expect(spy).toHaveBeenCalledTimes(4);
      expect(spy).toHaveBeenNthCalledWith(1, expect.objectContaining(JOB_SUB_1_SUCCESS_RESULT));
      expect(spy).toHaveBeenNthCalledWith(2, expect.objectContaining(JOB_SUB_2_SUCCESS_RESULT));
      expect(spy).toHaveBeenNthCalledWith(3, expect.objectContaining(JOB_SUB_3_SUCCESS_RESULT));
      expect(spy).toHaveBeenNthCalledWith(4, expect.objectContaining(JOB_SUB_4_SUCCESS_RESULT));
      expect(processBatchSpy).toHaveBeenCalledTimes(2);
      expect(processBatchSpy).toHaveBeenNthCalledWith(1, [submissions[0], submissions[1], submissions[2]]);
      expect(processBatchSpy).toHaveBeenNthCalledWith(2, [submissions[3]]);
    });

    it('should handle simultaneous job submissions correctly (with 2 batches worth)', async () => {
      const callback = jest.fn();
      const processBatchSpy = jest.spyOn(mockBatchProcessor, 'processBatch');
      const jobs: JobSubmission<any>[] = [JOB_SUB_1, JOB_SUB_2, JOB_SUB_3, JOB_SUB_4, JOB_SUB_5, JOB_SUB_6];

      const submissions = await Promise.all(jobs.map((job) => batcher.submitJob(job, callback)));
      await wait(PROCESSING_WAIT_TIME * 2); // Wait for processing to complete

      expect(callback).toHaveBeenCalledTimes(6);
      expect(callback).toHaveBeenNthCalledWith(1, expect.objectContaining(JOB_SUB_1_SUCCESS_RESULT));
      expect(callback).toHaveBeenNthCalledWith(2, expect.objectContaining(JOB_SUB_2_SUCCESS_RESULT));
      expect(callback).toHaveBeenNthCalledWith(3, expect.objectContaining(JOB_SUB_3_SUCCESS_RESULT));
      expect(callback).toHaveBeenNthCalledWith(4, expect.objectContaining(JOB_SUB_4_SUCCESS_RESULT));
      expect(callback).toHaveBeenNthCalledWith(5, expect.objectContaining(JOB_SUB_5_SUCCESS_RESULT));
      expect(callback).toHaveBeenNthCalledWith(6, expect.objectContaining(JOB_SUB_6_SUCCESS_RESULT));
      expect(processBatchSpy).toHaveBeenCalledTimes(2);
      expect(processBatchSpy).toHaveBeenNthCalledWith(1, [submissions[0], submissions[1], submissions[2]]);
      expect(processBatchSpy).toHaveBeenNthCalledWith(2, [submissions[3], submissions[4], submissions[5]]);
    });

    it('should handle simultaneous duplicate job submissions correctly', async () => {
      const callback = jest.fn();

      // Multiple duplicates of JOB_SUB_2 - Should ignore the duplicates and run the JOB_SUB_2 once
      const jobs: JobSubmission<any>[] = [JOB_SUB_1, JOB_SUB_2, JOB_SUB_2, JOB_SUB_3, JOB_SUB_2, JOB_SUB_2];

      await Promise.all(jobs.map((job) => batcher.submitJob(job, callback)));

      await wait(PROCESSING_WAIT_TIME); // Wait for processing to complete
      expect(callback).toHaveBeenCalledTimes(3);
      expect(callback).toHaveBeenNthCalledWith(1, expect.objectContaining(JOB_SUB_1_SUCCESS_RESULT));
      expect(callback).toHaveBeenNthCalledWith(2, expect.objectContaining(JOB_SUB_2_SUCCESS_RESULT));
      expect(callback).toHaveBeenNthCalledWith(3, expect.objectContaining(JOB_SUB_3_SUCCESS_RESULT));
    });
  });
});
