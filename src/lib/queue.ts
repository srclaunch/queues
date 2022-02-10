import {
  Exception,
  ExceptionsClient,
  KillProcessException,
} from '@srclaunch/exceptions';
import Logger from '@srclaunch/logger';
import { DefaultLogLevels, QueueInitializationResult } from '@srclaunch/types';
import {
  Queue as BullQueue,
  QueueEvents,
  QueueOptions,
  QueueScheduler,
} from 'bullmq';
import { DateTime } from 'luxon';
import { v4 } from 'uuid';

import { Task } from './task';
import { Worker } from './worker';

class QueueWithoutWorkersException extends Exception {}

export class Queue {
  config: QueueOptions;
  created: DateTime;
  description: string;
  events?: QueueEvents;
  exceptionsClient: ExceptionsClient;
  id: string;
  logger: DefaultLogLevels;
  name: string;
  queue?: BullQueue;
  scheduler?: QueueScheduler;
  workers?: Worker[];

  constructor({
    config,
    description,
    name,
    workers,
  }: {
    config: QueueOptions;
    description: string;
    name: string;
    workers: Worker[];
  }) {
    this.id = v4();
    this.created = DateTime.now();
    this.name = name;
    this.description = description;
    this.workers = workers;
    this.config = config;
    this.logger = new Logger();

    this.exceptionsClient = new ExceptionsClient({
      processExceptionsHandler: async exception =>
        await this.gracefulExit(exception),
      processInteruptHandler: async () => await this.gracefulExit(),
      processTerminationHandler: async () => await this.gracefulExit(),
    });

    try {
      this.queue = new BullQueue(name, config);
      this.events = new QueueEvents(name, config);
      this.scheduler = new QueueScheduler(name, config);

      this.addEventListeners();
    } catch (err) {
      this.logger.exception(`Caught exception: ${err.name}`, err, {
        tags: {
          file: 'lib/queue.ts',
          func: 'Queue.constructor()',
          type: 'CaughtException',
          worker: name,
        },
      });
    }
  }

  public async initialize(): Promise<QueueInitializationResult> {
    try {
      this.logger.info(`Initializing worker queue: ${this.name}...`);

      if (this.workers && this.workers.length > 0) {
        for (const worker of this.workers) {
          if (worker.file) {
            this.logger.info(`Initializing worker: ${worker.name}...`);

            const bullWorker = new Worker({
              description: worker.description,
              file: worker.file,
              name: worker.name,
              options: worker.options,
            });

            bullWorker.on('failed', (job, err) => {
              this.logger.exception(`Worker failure: ${err.name}`, err, {
                tags: {
                  file: 'lib/queue.ts',
                  func: 'initialize()',
                  job: job.id,
                  queue: this.name,
                  worker: worker.name,
                },
              });
            });

            bullWorker.on('error', err => {
              this.logger.exception(`Worker error: ${err.name}`, err, {
                tags: {
                  file: 'lib/queue.ts',
                  func: 'initialize()',
                  queue: this.name,
                  worker: worker.name,
                },
              });
            });
          }
        }

        this.logger.info('Worker queues initialized.');

        return {
          error: false,
        };
      }

      throw new QueueWithoutWorkersException(
        `No workers found for queue "${this.name}"`,
        {
          file: 'lib/queue.ts',
          func: 'intialize()',
        },
      );
    } catch (err) {
      this.logger.exception(`Caught exception: ${err.name}`, err, {
        tags: {
          file: 'lib/queue.ts',
          func: 'initialize()',
          type: 'CaughtException',
          worker: this.name,
        },
      });

      return {
        error: err,
      };
    }
  }

  public async addTask(job: Task): Promise<void> {
    await this.queue?.add(this.name, job.toJson());
  }

  public async addTasks(jobs: Task[]): Promise<void> {
    const formattedTasks = jobs.map(job => job.toJson());

    await this.queue?.addBulk(formattedTasks);
  }

  private addEventListeners(): void {
    if (!this.events) return;

    this.events.on(
      'active',
      ({ jobId, prev }: { jobId: string; prev?: string }, id: string) => {
        this.logger.info(`Job active: ${jobId}`, {
          tags: {
            job: jobId,
            queue: this.name,
            worker: id,
          },
        });
      },
    );

    this.events.on(
      'completed',
      (
        {
          jobId,
          returnvalue,
          prev,
        }: {
          jobId: string;
          returnvalue?: string;
          prev?: string | undefined;
        },
        id: string,
      ) => {
        this.logger.info(`Job completed: ${jobId}`, {
          tags: {
            job: jobId,
            queue: this.name,
            worker: id,
          },
        });
      },
    );

    this.events.on(
      'delayed',
      ({ jobId, delay }: { jobId: string; delay: number }, id: string) => {
        this.logger.info(
          `Job delayed: ${jobId} (Delay = ${delay.toString()})`,
          {
            tags: {
              job: jobId,
              queue: this.name,
              worker: id,
            },
          },
        );
      },
    );

    this.events.on('drained', id => {
      this.logger.info(`Queue drained: ${this.name}`, {
        tags: {
          queue: this.name,
        },
      });
    });

    this.events.on(
      'failed',
      (
        {
          jobId,
          failedReason,
          prev,
        }: { jobId: string; failedReason: string; prev: string },
        id: string,
      ) => {
        this.logger.warning(`Job failed: ${jobId} (Reason = ${failedReason})`, {
          tags: {
            job: jobId,
            queue: this.name,
            worker: id,
          },
        });
      },
    );

    this.events.on(
      'progress',
      (
        { jobId, progress }: { jobId: string; progress: string },
        id: string,
      ) => {
        this.logger.info(`Job progress: ${jobId} (Progress = ${progress})`, {
          tags: {
            job: jobId,
            queue: this.name,
            worker: id,
          },
        });
      },
    );

    this.events.on('stalled', ({ jobId }, id) => {
      this.logger.info(`Job stalled: ${jobId}`, {
        tags: {
          job: jobId,
          queue: this.name,
          worker: id,
        },
      });
    });

    this.events.on('removed', ({ jobId }, id) => {
      this.logger.info(`Job removed: ${jobId}`, {
        tags: {
          job: jobId,
          queue: this.name,
          worker: id,
        },
      });
    });

    this.events.on('waiting', ({ jobId }, id) => {
      this.logger.info(`Job waiting: ${jobId}`, {
        tags: {
          job: jobId,
          queue: this.name,
          worker: id,
        },
      });
    });
  }

  public async gracefulExit(exception?: Exception): Promise<void> {
    this.logger.info(
      `Gracefully shutting down ${this?.queue?.name ?? 'unknown queue'} `,
    );

    if (this.workers && this.workers.length > 0) {
      for (const worker of this.workers) {
        await worker.close();
      }
    }

    if (this.queue) await this.queue.close();

    if (this.events) await this.events.close();

    if (this.scheduler) await this.scheduler.close();

    this.logger.info(`Killing queue process... Goodbye.'} `);

    throw new KillProcessException('Shutting down gracefully', {
      file: 'lib/queue.ts',
      func: 'gracefulExit()',
    });
  }

  toJson(): {
    created: DateTime;
    description: string;
    events?: QueueEvents;
    id: string;
    name: string;
    queue?: BullQueue;
    scheduler?: QueueScheduler;
    workers?: Worker[];
  } {
    return {
      created: this.created,
      description: this.description,
      events: this.events,
      id: this.id,
      name: this.name,
      scheduler: this.scheduler,
      workers: this.workers,
    };
  }
}

//   logException(
//     new Exception(`[Queue: ${this.name}] Error: ${failedReason}`, {
//       file: 'lib/queue.ts',
//       func: 'addEventListeners()',
//     }),
//     {
//       data: {
//         failedReason,
//         job,
//       },
//       level: LogLevel.Critical,
//       message: `Error in queue ${this.name}`,
//       tags: {
//         file: 'lib/queue.ts',
//         func: 'queue.on("failed")',
//         queue: this.name,
//         type: 'CaughtException',
//       },
//     },
//   );
// });
