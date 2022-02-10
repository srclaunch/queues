import { Task as TaskT, TaskStatus } from '@srclaunch/types';
import { DateTime } from 'luxon';
import { v4 } from 'uuid';

export class Task {
  created: DateTime;
  data: TaskT[];
  id: string;
  name: string;
  status?: TaskStatus;

  constructor({ data, name }: { name: string; data: TaskT[] }) {
    this.id = v4();
    this.name = name;
    this.data = data;
    this.created = DateTime.now();
    this.status = TaskStatus.Created;
  }

  toJson() {
    return {
      created: this.created,
      data: this.data,
      id: this.id,
      name: this.name,
      status: this.status,
    };
  }
}
