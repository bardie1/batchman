# Batchman 
I simple and highly configurable micro batching library.

![Logo](https://assets.peachystyling.com.au/batchman.gif)

## Hey Upguard 👋
Thank you for stopping by. I hope you enjoy reading through Batchman.

## Tech Stack

Batchman was built as a NodeJS library. Other technologies used:

- Typescript
- Jest
- Async Mutex (async-mutex)

## Features

- Single or multi job types
- Batch processor agnostic. BYOP (Bring Your Own Processor)
- Configurable batch sizes and frequency
- Group jobs by key

## Future Improvements

- More robust concurrency with worker threads
- Create a batch id to pass into the batch processor. Could be used as a kind of trace id
- Add Retry functionality for failed jobs, with retry count.
- Extend retry functionality for dynamic retry wait time.
- Consider an event driven approach instead of callbacks
    - Callbacks are great for controll over handling results when specific job submitted
    - Callbacks could be unessary for some generic jobs
    - Event Emitter approach great for extensibility
    - Event Emmitter also great for decoupling and keeping the handling of events in a single place


## Usage/Examples

### Simple Usage

In this example, assume we want to create batch processing of logs. To use Batchman we need to bring our own processor. The processor must implement the ```BatchProcessor``` interface.

```typescript
interface BatchProcessor<T, R> {
  processBatch: (jobs: Job<T>[]) => Promise<JobResult<T, R>[]>;
}

class LogBatchProcessor implements BatchProcessor<LogPayload, boolean> {
  async processBatch(jobs: Job<LogPayload>[]): Promise<Array<JobResult<LogPayload, boolean>>> {
    const results = jobs.map((job) => {
      const logMessage = `${job.payload.timestamp} - ${job.payload.message}`;
      switch (job.payload.type) {
        case 'error':
          console.error(logMessage);
          break;
        case 'info':
          console.log(logMessage);
          break;
        case 'warning':
          console.warn(logMessage);
          break;

        default:
          console.log(logMessage);
          break;
      }

      const result: JobResult<LogPayload, boolean> = {
        ...job,
        result: true,
        completedAt: new Date(),
        errorReason: null,
      };

      return result;
    });
    return results;
  }
}
```

Great! Now we can use our batch processor with Batchman.

```typescript
type LogType = 'error' | 'info' | 'warning';

interface LogPayload {
  timestamp: Date;
  message: string;
  type: LogType;
}

const logProcessorFactory = () => new LogBatchProcessor();

const logBatchman = new Batchman<LogPayload, boolean>({}, logProcessorFactory);

  await logBatchman.submitJob(
    {
      id: '1',
      key: 'log',
      payload: {
        timestamp: new Date(),
        message: 'Hello',
        type: 'info',
      },
    },
    async (res: JobResult<LogPayload, boolean>) => {
      // Handle the completed job in async way
    },
  );

  await logBatchman.submitJob(
    {
      id: '2',
      key: 'log',
      payload: {
        timestamp: new Date(),
        message: 'World',
        type: 'warning',
      },
    },
    (res: JobResult<LogPayload, boolean>) => {
      // Handle the completed in sync way
    },
  );

  await logBatchman.shutdown();
```

### Multi Key Jobs

Not all processors are that simple. We may want to process jobs differently based on some criteria. For this we can use keys to differentiate between jobs.

Using a multi key feature prevents us from having to create multiple instances of Batchman for different jobs.

For this example let's assume that we want to have 3 batchers.
1. Our logger one from above
2. A batcher that handles extremely time sensitive and mission critical jobs
3. A analytics batcher ( less mission critical and we can process alot in one api call )

Here is how you can use Batchman to house all 3 without creating multiple instances. (Multiple instances are created under the hood, but this is abstracted away).

```typescript

    /**
   *
   * EXAMPLE PROCESSORS
   */
  async function LoggerProcessor(jobs: Job<any>[]): Promise<JobResult<any, any>[]> {
    console.log(jobs);
    // Your logger processor
    const results: JobResult<any, any>[] = [];

    return results;
  }
  async function TimeSensitiveProcessor(jobs: Job<any>[]): Promise<JobResult<any, any>[]> {
    console.log(jobs);
    // Do time sensitive processing
    const results: JobResult<any, any>[] = [];

    return results;
  }
  async function AnalyticsProcessor(jobs: Job<any>[]): Promise<JobResult<any, any>[]> {
    console.log(jobs);
    // Do analytics processing
    const results: JobResult<any, any>[] = [];

    return results;
  }

  async function DefaultProcessor(jobs: Job<any>[]): Promise<JobResult<any, any>[]> {
    console.log(jobs);
    // Do default processing
    const results: JobResult<any, any>[] = [];

    return results;
  }
  /**
   *
   * EXAMPLE PROCESSORS
   */

```

And then we can create and use batchman.

```typescript
const batchman = new Batchman<any, any>(
    {
      frequency: 5_000,
      maxBatchSize: 20,
      debug: false,
      useInstancePerKey: true, // use this to notify batchman to use different instances and group the jobs by key
      keyConfigOverrides: [
        {
          //Omission of any configs will result in using the default config value
          key: 'logger',
          frequency: 10_000,
        },
        {
          key: 'time-sensitive',
          frequency: 500,
          maxBatchSize: 4,
        },
        {
          key: 'analytics',
          frequency: 60_000,
          maxBatchSize: 500,
        },
      ],
    }, // We can also pass in a map of processor factories that return our processors for a specific. It must contain a 'default'
    new Map([
      ['default', () => ({ processBatch: DefaultProcessor })],
      ['logger', () => ({ processBatch: LoggerProcessor })],
      ['time-sensitive', () => ({ processBatch: TimeSensitiveProcessor })],
      ['analytics', () => ({ processBatch: AnalyticsProcessor })],
    ]),
  );

  //We can now submit jobs with keys

  await batchman.submitJob(
    {
      id: '1',
      key: 'logger',
      payload: 'This is a log message',
    },
    async (res: JobResult<any, any>) => {
      // Handle the completed job in async way
    },
  );

  await batchman.submitJob(
    {
      id: '1', // Job ID's only have to be unique per key
      key: 'time-sensitive',
      payload: 'This is a log message',
    },
    async (res: JobResult<any, any>) => {
      // Handle the completed job in async way
    },
  );

  //You can now submit jobs and they will use their respective frequencies, batch sizes and processors
  // if useInstancePerKey is true and a job is submitted with a key not defined in the overrides or processors it will create a new instance with the default configurations.
```

## Configs
Here is a breakdown of the configuration of batchman

### ```BatchmanConfig```

| Option         | Type     | Description                                         | Default Value | Required |
|----------------|----------|-----------------------------------------------------|---------------|----------|
| `frequency`   | `number` | How often to process batches                         |  `5000`            | No       |
| `maxBatchSize`  | `number` | Max number of jobs to be processed at once          | `10`         | No      |
| `debug`     | `boolean` | Whether to enable loggin or not                        | `false`      | No      |
| `useInstancePerKey`  | `boolean`| Whether to create new instances for each job key | `true`        | No       |
| `keyConfigOverrides` | `array`  | Array of Batchman configs for each key            | `[]`          | No       |


## Running Batchman

### Cloning the Repository
To get started with the batcher library, first clone the repository to your local machine:

```sh
git clone https://github.com/bardie1/batchman.git
cd batchman
```

### Installing Dependencies
Once you have cloned the repository, navigate to the project directory and install the necessary dependencies using `npm`

```sh
npm install
```

### Running Tests
To ensure that everything is set up correctly and to run the test suite, use the following commands:

```sh
npm run test -- --coverage
```

## Chat Soon
I look forward to chatting with the team soon!