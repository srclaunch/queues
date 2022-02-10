import { Worker as BullWorker, WorkerOptions } from 'bullmq';

export class Worker extends BullWorker {
  file: string;
  description: string;
  options?: WorkerOptions;
  name: string;

  constructor({
    file,
    description,
    name,
    options,
  }: {
    description: string;
    file: string;
    options?: WorkerOptions;
    name: string;
  }) {
    super(name, file, options);

    this.name = name;
    this.description = description;
    this.file = file;
    this.options = options;
  }
}
