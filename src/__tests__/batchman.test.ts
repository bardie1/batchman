import { BatchProcessor, Job, JobResult, JobResultCallback, JobStatus, JobSubmission } from '../types';
import {
  BatchProcessorFactory,
  Batchman,
  DEFAULT_DEBUG,
  DEFAULT_FREQUENCY,
  DEFAULT_KEYCONFIGOVERRIDES,
  DEFAULT_MAX_BATCH_SIZE,
  DEFAULT_USEINSTANCEPERKEY,
  MicroBatcher,
} from '../lib';
import { BatchmanConfig } from '../types/config';
import { wait } from '../utils/wait';

const OriginalMicroBatcher = jest.requireActual('../lib/microBatcher').MicroBatcher as jest.MockedClass<
  typeof MicroBatcher
>;
jest.mock('../lib/microBatcher', () => {
  return {
    MicroBatcher: jest.fn().mockImplementation((config, batchProcessor) => {
      const instance = new OriginalMicroBatcher(config, batchProcessor);
      jest.spyOn(instance, 'submitJob'); // Spy on instance method
      jest.spyOn(instance, 'shutdown'); // Spy on instance method
      jest.spyOn(instance as any, 'processBatch');
      jest.spyOn(instance as any, 'log');
      jest.spyOn(instance as any, 'debugLog');
      return instance;
    }),
  };
});

MicroBatcher.MicroBatcherConfigFromBatchmanConfig = jest.fn(OriginalMicroBatcher.MicroBatcherConfigFromBatchmanConfig);
MicroBatcher.validateConfig = jest.fn(OriginalMicroBatcher.validateConfig);

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

function batchProcessorFactory() {
  return new MockBatchProcessor();
}

const JOB_SUB_1 = { id: '1', key: 'default', payload: 10 };
const JOB_SUB_2 = { id: '2', key: 'default', payload: 20 };
const JOB_SUB_3 = { id: '3', key: 'default', payload: 30 };
const JOB_SUB_4 = { id: '4', key: 'default', payload: 40 };

const DEFAULT_SUCCESS_RESULT = {
  result: 'success',
  status: 'completed' as JobStatus,
  errorReason: null,
  submittedAt: expect.any(Date),
  completedAt: expect.any(Date),
};

const JOB_SUB_1_SUCCESS_RESULT = { ...JOB_SUB_1, ...DEFAULT_SUCCESS_RESULT };
const JOB_SUB_2_SUCCESS_RESULT = { ...JOB_SUB_2, ...DEFAULT_SUCCESS_RESULT };
const JOB_SUB_3_SUCCESS_RESULT = { ...JOB_SUB_3, ...DEFAULT_SUCCESS_RESULT };
const JOB_SUB_4_SUCCESS_RESULT = { ...JOB_SUB_4, ...DEFAULT_SUCCESS_RESULT };

const BATCH_SIZE = 3;
const FREQUENCY = 1000;

const DEFAULT_CONFIG: Required<BatchmanConfig> = {
  frequency: FREQUENCY,
  maxBatchSize: BATCH_SIZE,
  useInstancePerKey: true,
  debug: false,
  keyConfigOverrides: [],
};

describe('Batchman', () => {
  let config: Required<BatchmanConfig>;
  let batchProcessor: BatchProcessorFactory<number, string>;

  beforeEach(() => {
    config = DEFAULT_CONFIG;
    batchProcessor = batchProcessorFactory;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('Config Validation', () => {
    describe('validateConfig', () => {
      it('should throw an error if frequency is less than or equal to 0', () => {
        expect(() => Batchman.validateConfig({ ...DEFAULT_CONFIG, frequency: 0 }, batchProcessor)).toThrow(
          'Frequency must be greater than 0',
        );
      });

      it('should throw an error if maxBatchSize is less than or equal to 0', () => {
        expect(() => Batchman.validateConfig({ ...DEFAULT_CONFIG, maxBatchSize: 0 }, batchProcessor)).toThrow(
          'Max batch size must be greater than 0',
        );
      });

      it('should throw an error if useInstancePerKey is false but multiple processors are provided', () => {
        const batchProcessors = new Map<string, BatchProcessorFactory<number, string>>([['id', batchProcessorFactory]]);
        expect(() => Batchman.validateConfig({ ...DEFAULT_CONFIG, useInstancePerKey: false }, batchProcessors)).toThrow(
          'Multiple Batch processors can only be used when useInstancePerKey is true',
        );
      });

      it('should throw an error if useInstancePerKey is false but multiple configs are provided', () => {
        expect(() =>
          Batchman.validateConfig(
            { ...DEFAULT_CONFIG, useInstancePerKey: false, keyConfigOverrides: [{ ...DEFAULT_CONFIG, key: 'other' }] },
            batchProcessor,
          ),
        ).toThrow('Cannot use keyConfigOverrides when useInstancePerKey is false');
      });

      describe('validateConfigs for keyConfigOverrides', () => {
        it('should throw an error if frequency is less than or equal to 0', () => {
          expect(() =>
            Batchman.validateConfig(
              { ...DEFAULT_CONFIG, keyConfigOverrides: [{ ...DEFAULT_CONFIG, frequency: 0, key: 'other' }] },
              batchProcessor,
            ),
          ).toThrow('Frequency must be greater than 0');
        });

        it('should throw an error if maxBatchSize is less than or equal to 0', () => {
          expect(() =>
            Batchman.validateConfig(
              { ...DEFAULT_CONFIG, keyConfigOverrides: [{ ...DEFAULT_CONFIG, maxBatchSize: 0, key: 'other' }] },
              batchProcessor,
            ),
          ).toThrow('Max batch size must be greater than 0');
        });

        it('should use default processor if processor for key is not provided', () => {
          const defaultBatchProcessor = jest.fn();
          const defaultBatchProcessorFactory = jest.fn(() => {
            return {
              processBatch: defaultBatchProcessor,
            };
          });
          const batchProcessorMap = new Map([
            ['other', batchProcessor],
            ['default', defaultBatchProcessorFactory],
          ]);

          expect(() =>
            Batchman.validateConfig(
              { ...DEFAULT_CONFIG, keyConfigOverrides: [{ ...DEFAULT_CONFIG, maxBatchSize: 20, key: 'noprocessor' }] },
              batchProcessorMap,
            ),
          ).not.toThrow();
          expect(MicroBatcher.validateConfig).toHaveBeenCalledWith(
            expect.any(Object),
            expect.objectContaining({
              processBatch: defaultBatchProcessor,
            }),
          );
        });

        it('should throw error if no processor is found', () => {
          const key = 'noprocessor';
          const defaultBatchProcessor = jest.fn();
          const defaultBatchProcessorFactory = jest.fn(() => {
            return {
              processBatch: defaultBatchProcessor,
            };
          });
          const batchProcessorMap = new Map([
            ['other', batchProcessor],
            ['more', defaultBatchProcessorFactory],
          ]);

          expect(() =>
            Batchman.validateConfig(
              { ...DEFAULT_CONFIG, keyConfigOverrides: [{ ...DEFAULT_CONFIG, maxBatchSize: 20, key: key }] },
              batchProcessorMap,
            ),
          ).toThrow(`No batch processor found for key ${key}`);
        });
      });
    });

    describe('validateBatchProcessorConfig', () => {
      it('should throw an error if the factory does not return an object that has a processBatch function', () => {
        //@ts-expect-error - Testing invalid input
        expect(() => Batchman.validateBatchProcessorConfig(() => ({}))).toThrow(
          'Batch processor must have a processBatch function',
        );
      });

      it('should throw an error if the factory map doesnt contain a default', () => {
        //@ts-expect-error - Testing invalid input
        expect(() => Batchman.validateBatchProcessorConfig<number, string>(new Map([['key', () => ({})]]))).toThrow(
          'Batch processor must have a default processor',
        );
      });

      it('should throw an error if the factory map values do not return an object that has a processBatchFunction', () => {
        //@ts-expect-error - Testing invalid input
        expect(() => Batchman.validateBatchProcessorConfig<number, string>(new Map([['default', () => ({})]]))).toThrow(
          'Batch processor must have a processBatch function',
        );
      });
    });
  });

  it('should create new instances of Batchman with the correct defaults', () => {
    const batchman = new Batchman({}, batchProcessor);

    expect(batchman).toBeInstanceOf(Batchman);
    expect((batchman as any).config).toEqual({
      frequency: DEFAULT_FREQUENCY,
      maxBatchSize: DEFAULT_MAX_BATCH_SIZE,
      useInstancePerKey: DEFAULT_USEINSTANCEPERKEY,
      debug: DEFAULT_DEBUG,
      keyConfigOverrides: DEFAULT_KEYCONFIGOVERRIDES,
    });

    expect(MicroBatcher).toHaveBeenCalledWith(
      expect.objectContaining(
        MicroBatcher.MicroBatcherConfigFromBatchmanConfig({
          frequency: DEFAULT_FREQUENCY,
          maxBatchSize: DEFAULT_MAX_BATCH_SIZE,
          debug: DEFAULT_DEBUG,
        }),
      ),
      expect.objectContaining({
        processBatch: expect.any(Function),
      }),
    );
  });

  it('should correctly assign and process jobs', async () => {
    const batchman = new Batchman<number, string>(config, batchProcessor);

    const callback: JobResultCallback<any, any> = jest.fn();

    await batchman.submitJob(JOB_SUB_1, callback);

    await wait(config.frequency + 1);

    const microBatcherInstance = (batchman as any).microBatcherInstances.get('default');
    expect(microBatcherInstance.submitJob).toHaveBeenCalledWith(JOB_SUB_1, callback);
  });

  it('should correctly assign and process jobs (not split by key)', async () => {
    const batchman = new Batchman<number, string>({ ...config, useInstancePerKey: false }, batchProcessor);
    const callback: JobResultCallback<any, any> = jest.fn();

    await batchman.submitJob(JOB_SUB_1, callback);
    await batchman.submitJob({ ...JOB_SUB_2, key: 'new-key' }, callback);
    const instances = (batchman as any).microBatcherInstances.size;
    const defaultInstance = (batchman as any).microBatcherInstances.get('default');
    expect(instances).toBe(1);
    expect(defaultInstance.jobs.length).toBe(2);
  });

  it('should be able to retrieve a job by id and key', async () => {
    const batchProcessor = {
      processBatch: async () => {
        await wait(500);
        return Promise.resolve([{ ...JOB_SUB_1_SUCCESS_RESULT, completedAt: new Date(), submittedAt: new Date() }]);
      },
    };

    const batchProcessorFactory = jest.fn(() => batchProcessor);

    const batchman = new Batchman<number, string>(config, batchProcessorFactory);
    const callback: JobResultCallback<any, any> = jest.fn();

    const job = await batchman.submitJob(JOB_SUB_1, callback);
    const submittedJob = batchman.getJob(JOB_SUB_1.key, JOB_SUB_1.id);
    expect(submittedJob).toEqual(job);

    await wait(config.frequency + 1);

    const retrievedDuring = batchman.getJob(JOB_SUB_1.key, JOB_SUB_1.id);
    expect(retrievedDuring).toEqual({
      ...job,
      status: 'processing',
    });

    await wait(500);

    const retrievedAfter = batchman.getJob(JOB_SUB_1.key, JOB_SUB_1.id);
    expect(retrievedAfter).toBeNull(); //Because it has been processed and is no longer in the batcher
  });

  it('should be return null when getting a job if useInstancePerKey is true and the instance doesnt exist for that key', async () => {
    const callback = jest.fn();
    const batchProcessor = {
      processBatch: async () => {
        await wait(500);
        return Promise.resolve([{ ...JOB_SUB_1_SUCCESS_RESULT, completedAt: new Date(), submittedAt: new Date() }]);
      },
    };

    const batchProcessorFactory = jest.fn(() => batchProcessor);

    const batchman = new Batchman<number, string>(config, batchProcessorFactory);

    const job = await batchman.submitJob({ ...JOB_SUB_1, key: 'different-key' }, callback);
    const retrievedJob = batchman.getJob('different-key', JOB_SUB_1.id);
    expect(retrievedJob).toEqual(job);
    (batchman as any).microBatcherInstances.delete('different-key');
    const noBatcherInstanceJob = batchman.getJob('different-key', JOB_SUB_1.id);
    expect(noBatcherInstanceJob).toBeNull();
  });

  it('should be return the correct job based on key and id from default instance when useInstancePerKey is false', async () => {
    const callback = jest.fn();
    const batchProcessor = {
      processBatch: async () => {
        await wait(500);
        return Promise.resolve([{ ...JOB_SUB_1_SUCCESS_RESULT, completedAt: new Date(), submittedAt: new Date() }]);
      },
    };

    const batchProcessorFactory = jest.fn(() => batchProcessor);

    const batchman = new Batchman<number, string>({ ...config, useInstancePerKey: false }, batchProcessorFactory);

    const job = await batchman.submitJob({ ...JOB_SUB_1, key: 'different-key' }, callback);
    const retrievedJob = batchman.getJob('different-key', JOB_SUB_1.id);
    expect(retrievedJob).toEqual(job);

    await wait(config.frequency + 1);

    const retrievedDuring = batchman.getJob('different-key', JOB_SUB_1.id);
    expect(retrievedDuring).toEqual({
      ...job,
      status: 'processing',
    });

    await wait(500);

    const retrievedAfter = batchman.getJob('different-key', JOB_SUB_1.id);
    expect(retrievedAfter).toBeNull(); //Because it has been processed and is no longer in the batcher
  });

  it('should skip job submission during shutdown', async () => {
    const batchman = new Batchman(config, batchProcessor);
    const callback: JobResultCallback<any, any> = jest.fn();

    await batchman.shutdown();
    const res = await batchman.submitJob(JOB_SUB_1, callback);
    expect(res).toEqual({
      ...JOB_SUB_1,
      status: 'failed',
      errorReason: 'shutdown_batch',
      submittedAt: expect.any(Date),
      errorMessage: `Batcher is shutting down`,
    });
  });

  it('should handle concurrent job submissions', async () => {
    const batchman = new Batchman(config, batchProcessor);
    const jobs = [JOB_SUB_1, JOB_SUB_2, JOB_SUB_3];
    const callbacks = [jest.fn(), jest.fn(), jest.fn()];

    await Promise.all(jobs.map((job, index) => batchman.submitJob(job, callbacks[index])));

    const microBatcherInstance = (batchman as any).microBatcherInstances.get('default');
    jobs.forEach((job, index) => {
      expect(microBatcherInstance.submitJob).toHaveBeenNthCalledWith(index + 1, job, callbacks[index]);
    });
  });

  it('should use custom configurations for specific keys', async () => {
    const customConfig = { maxBatchSize: 2, frequency: 500, debug: true };

    const expectedConfig = MicroBatcher.MicroBatcherConfigFromBatchmanConfig({
      ...config,
      ...customConfig,
    });

    const batchProcessorMap = new Map([
      ['default', batchProcessor],
      ['customKey', batchProcessor],
    ]);
    const batchman = new Batchman(
      { ...config, keyConfigOverrides: [{ ...customConfig, key: 'customKey' }] },
      batchProcessorMap,
    );

    const job: JobSubmission<number> = { id: '1', key: 'customKey', payload: 10 };
    const callback: JobResultCallback<any, any> = jest.fn();

    await batchman.submitJob(job, callback);

    expect(MicroBatcher).toHaveBeenLastCalledWith(
      expect.objectContaining(expectedConfig),
      expect.objectContaining({
        processBatch: expect.any(Function),
      }),
    );

    const microBatcherInstance = (batchman as any).microBatcherInstances.get('customKey');
    expect(microBatcherInstance.submitJob).toHaveBeenCalledTimes(1);
    expect(microBatcherInstance.submitJob).toHaveBeenNthCalledWith(1, job, callback);
  });

  it('should process jobs according to max batch size', async () => {
    const customConfig = { ...config, maxBatchSize: 2 };
    const newBatchMan = new Batchman(customConfig, batchProcessor);

    const jobs = [JOB_SUB_1, JOB_SUB_2, JOB_SUB_3, JOB_SUB_4];
    const callback = jest.fn();

    await Promise.all(jobs.map((job) => newBatchMan.submitJob(job, callback)));

    await wait(config.frequency + 1);

    const microBatcherInstance = (newBatchMan as any).microBatcherInstances.get('default');
    expect(microBatcherInstance.submitJob).toHaveBeenCalledTimes(4);
    expect(microBatcherInstance.processBatch).toHaveBeenCalledTimes(2);
    expect(callback).toHaveBeenCalledTimes(4);
    expect(callback).toHaveBeenNthCalledWith(1, expect.objectContaining(JOB_SUB_1_SUCCESS_RESULT));
    expect(callback).toHaveBeenNthCalledWith(2, expect.objectContaining(JOB_SUB_2_SUCCESS_RESULT));
    expect(callback).toHaveBeenNthCalledWith(3, expect.objectContaining(JOB_SUB_3_SUCCESS_RESULT));
    expect(callback).toHaveBeenNthCalledWith(4, expect.objectContaining(JOB_SUB_4_SUCCESS_RESULT));
  });

  it('should process jobs according to frequency', async () => {
    const batchman = new Batchman(config, batchProcessor);
    const callback: JobResultCallback<any, any> = jest.fn();

    await batchman.submitJob(JOB_SUB_1, callback);

    const microBatcherInstance = (batchman as any).microBatcherInstances.get('default');
    expect(microBatcherInstance.submitJob).toHaveBeenCalledWith(JOB_SUB_1, callback);

    await wait(config.frequency + 1);
    expect(microBatcherInstance.processBatch).toHaveBeenCalledTimes(1);
  });

  it('should shutdown all instances', async () => {
    const batchman = new Batchman(config, batchProcessor);

    const microBatcherInstance = (batchman as any).microBatcherInstances.get('default');
    await batchman.shutdown();

    expect(microBatcherInstance.shutdown).toHaveBeenCalledTimes(1);
  });

  it('should handle errors in batch processing gracefully', async () => {
    const errorProcessor = {
      processBatch: jest.fn().mockRejectedValue(new Error('Processing Error')),
    };
    const batchman = new Batchman(config, () => errorProcessor);
    const callback: JobResultCallback<any, any> = jest.fn();

    await batchman.submitJob(JOB_SUB_1, callback);

    const microBatcherInstance = (batchman as any).microBatcherInstances.get('default');
    await wait(config.frequency + 1);
    expect(errorProcessor.processBatch).toHaveBeenCalledTimes(1);
    expect(microBatcherInstance.debugLog).toHaveBeenCalledWith(
      expect.stringContaining('Error in batchProcessor: Processing Error'),
    );
  });

  it('should use the default batch processor when no specific processor is defined', async () => {
    const jobSubmission: JobSubmission<any> = { ...JOB_SUB_1, key: 'undefinedKey' };

    const defaultBatchProcessor = jest.fn(async () => [
      {
        ...jobSubmission,
        status: 'completed' as JobStatus,
        result: 'success',
        errorReason: null,
        submittedAt: new Date(),
        completedAt: new Date(),
      },
    ]);
    const defaultBatchProcessorFactory = jest.fn(() => {
      return {
        processBatch: defaultBatchProcessor,
      };
    });
    const batchProcessorMap = new Map([
      ['other', batchProcessor],
      ['default', defaultBatchProcessorFactory],
    ]);
    const batchman = new Batchman(config, batchProcessorMap);

    const callback: JobResultCallback<any, any> = jest.fn();

    await batchman.submitJob(jobSubmission, callback);

    await wait(config.frequency + 1);

    const expectedConfig = MicroBatcher.MicroBatcherConfigFromBatchmanConfig(config);

    const microBatherInstance = (batchman as any).microBatcherInstances.get('undefinedKey');
    expect(MicroBatcher).toHaveBeenNthCalledWith(
      2,
      expectedConfig,
      expect.objectContaining({
        processBatch: defaultBatchProcessor,
      }),
    );
    expect(microBatherInstance.submitJob).toHaveBeenCalledWith(jobSubmission, callback);
    expect(defaultBatchProcessor).toHaveBeenCalledTimes(1);
  });

  it('should use process all the jobs with same processor if only one is provided', async () => {
    const defaultBatchProcessor = jest.fn(async (jobs) =>
      jobs.map((job: Job<number>) => ({
        ...job,
        status: 'completed' as JobStatus,
        result: 'success',
        errorReason: null,
        submittedAt: new Date(),
        completedAt: new Date(),
      })),
    );
    const defaultBatchProcessorFactory = jest.fn(() => {
      return {
        processBatch: defaultBatchProcessor,
      };
    });

    const batchman = new Batchman(config, defaultBatchProcessorFactory);

    const callback: JobResultCallback<any, any> = jest.fn();

    await batchman.submitJob(JOB_SUB_1, callback);
    await batchman.submitJob({ ...JOB_SUB_2, key: 'different-key' }, callback);

    await wait(config.frequency + 100);

    const expectedConfig = MicroBatcher.MicroBatcherConfigFromBatchmanConfig(config);

    const microBatcherInstance = (batchman as any).microBatcherInstances.get('default');
    expect(MicroBatcher).toHaveBeenNthCalledWith(
      2,
      expectedConfig,
      expect.objectContaining({
        processBatch: defaultBatchProcessor,
      }),
    );
    expect(microBatcherInstance.submitJob).toHaveBeenCalledWith(JOB_SUB_1, callback);

    const microBatcherInstanceKey2 = (batchman as any).microBatcherInstances.get('different-key');
    expect(microBatcherInstanceKey2.submitJob).toHaveBeenCalledWith({ ...JOB_SUB_2, key: 'different-key' }, callback);
    expect(defaultBatchProcessor).toHaveBeenCalledTimes(2);
  });
});
