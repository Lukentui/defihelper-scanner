import { v4 as uuid } from "uuid";
import { Factory } from "@services/Container";
import { Task, TaskStatus, Table, Process, hasHandler } from "./Entity";
import * as Handlers from "../../queue";
import { Log } from "@services/Log";

export type Handler = keyof typeof Handlers;

export interface HandleOptions {
  include?: Handler[];
  exclude?: Handler[];
}

export interface BrokerOptions {
  interval: number;
  handler: HandleOptions;
}

export class Broker {
  protected isStarted: boolean = false;

  constructor(
    readonly service: QueueService = service,
    readonly options: Partial<BrokerOptions> = {}
  ) {
    this.options = {
      interval: 1000,
      ...options,
    };
  }

  protected async handle() {
    if (!this.isStarted) return;

    const res = await this.service.handle(this.options.handler);
    if (!res) {
      await new Promise((resolve) => {
        setTimeout(resolve, this.options.interval);
      });
    }

    this.handle();
  }

  start() {
    this.isStarted = true;
    this.handle();
  }

  stop() {
    this.isStarted = false;
  }
}

export class QueueService {
  constructor(readonly table: Factory<Table>, readonly log: Factory<Log>) {}

  async resetAndRestart(task: Task) {
    const updated = {
      ...task,
      status: TaskStatus.Pending,
      startAt: new Date(),
      error: "",
      updatedAt: new Date(),
    };
    await this.table().update(updated).where("id", updated.id);

    return updated;
  }

  async push<H extends Handler>(
    handler: H,
    params: Object,
    timeout: number|null = null,
    startAt: Date = new Date(),
  ) {
    const task: Task = {
      id: uuid(),
      handler,
      params,
      startAt,
      timeout,
      status: TaskStatus.Pending,
      info: "",
      error: "",
      retries: 0,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    await this.table().insert(task);

    return task;
  }

  async handle(options: HandleOptions = {}): Promise<boolean> {
    const current = await this.table()
      .where(function () {
        this.where("status", TaskStatus.Pending).andWhere(
          "startAt",
          "<=",
          new Date()
        );
        if (options.include && options.include.length > 0) {
          this.whereIn("handler", options.include);
        }
        if (options.exclude && options.exclude.length > 0) {
          this.whereNotIn("handler", options.exclude);
        }
      })
      .orderBy("startAt", "asc")
      .limit(1)
      .first();
    if (!current) return false;

    const lock = await this.table()
      .update({ status: TaskStatus.Process })
      .increment('retries')
      .where({
        id: current.id,
        status: TaskStatus.Pending,
      });
    if (lock === 0) return false;

    const process = new Process(current);
    try {
      this.log().info(`Handle task: ${current.id}`);
      const { task: result } = await Handlers[current.handler].default(process);
      await this.table().update(result).where("id", current.id);
    } catch (e) {
      await this.table().update(process.error(e).task).where("id", current.id);
    }

    return true;
  }

  createBroker(options: Partial<BrokerOptions> = {}) {
    if (typeof options.handler === "string" && !hasHandler(options.handler)) {
      throw new Error(`Invalid queue handler "${options.handler}"`);
    }
    return new Broker(this, options);
  }
}
