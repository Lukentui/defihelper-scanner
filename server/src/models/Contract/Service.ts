import container from "@container";
import { Factory } from "@services/Container";
import { Emitter } from "@services/Event";
import dayjs from "dayjs";
import { ethers } from "ethers";
import { v4 as uuid } from "uuid";
import {
  Contract,
  ContractTable,
  EventListener,
  EventListenerTable,
  eventListenerTableName,
  EventTable,
  eventTableName,
} from "./Entity";

async function abiResolver({ id, abi }: Contract) {
  if (abi !== null) return;

  return container.model.queueService().push("resolveAbi", { id });
}

async function eventResolver({ id }: EventListener) {
  return container.model
    .queueService()
    .push("resolveEvents", { id, step: 1000 });
}

export interface ContractStatisticsOptions {
  filter: {
    date?: {
      from: Date;
      to: Date;
    };
    block?: {
      from: number;
      to: number;
    };
  };
}

export class ContractService {
  constructor(
    readonly contractTable: Factory<ContractTable>,
    readonly listenerTable: Factory<EventListenerTable>,
    readonly eventTable: Factory<EventTable>
  ) {}

  public readonly onContractCreated = new Emitter<Contract>(abiResolver);

  public readonly onContractUpdated = new Emitter<{
    prev: Contract;
    current: Contract;
  }>(({ current }) => abiResolver(current));

  public readonly onContractDeleted = new Emitter<Contract>();

  public readonly onListenerCreated = new Emitter<EventListener>(eventResolver);

  async createContract(
    network: number,
    address: string,
    name: string,
    abi: ethers.ContractInterface | null,
    startHeight: number
  ) {
    const created = {
      id: uuid(),
      network,
      address: address.toLowerCase(),
      name,
      abi: abi === null ? null : JSON.stringify(abi, null, 4),
      startHeight,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    await this.contractTable().insert(created);
    this.onContractCreated.emit(created);

    return created;
  }

  async updateContract(contract: Contract) {
    const updated = {
      ...contract,
      address: contract.address.toLowerCase(),
      abi: contract.abi === null ? null : JSON.stringify(contract.abi, null, 4),
      updatedAt: new Date(),
    };
    await this.contractTable().where({ id: contract.id }).update(updated);
    this.onContractUpdated.emit({ prev: contract, current: updated });

    return updated;
  }

  async deleteContract(contract: Contract) {
    await this.contractTable().where({ id: contract.id }).delete();
    this.onContractDeleted.emit(contract);
  }

  async getContractStatistics(
    contract: Contract,
    options: ContractStatisticsOptions
  ) {
    const uniqueFrom = await this.eventTable()
      .distinct(`${eventTableName}.from`)
      .innerJoin(
        eventListenerTableName,
        `${eventListenerTableName}.id`,
        "=",
        `${eventTableName}.eventListener`
      )
      .where(function () {
        this.where(`${eventListenerTableName}.contract`, contract.id);
        if (options.filter) {
          const { date, block } = options.filter;
          if (date) {
            this.andWhereBetween(`${eventTableName}.createdAt`, [
              date.from,
              date.to,
            ]);
          }
          if (block) {
            this.andWhereBetween(`${eventTableName}.blockNumber`, [
              block.from,
              block.to,
            ]);
          }
        }
      });

    return {
      uniqueWalletsCount: uniqueFrom.length,
    };
  }

  async createListener(
    contract: Contract,
    name: string,
    syncHeight: number = contract.startHeight
  ) {
    const existing = await this.listenerTable()
      .where({
        name,
        contract: contract.id,
      })
      .first();

    if (existing) return existing;

    const created = {
      id: uuid(),
      contract: contract.id,
      name,
      syncHeight,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    await this.listenerTable().insert(created);
    this.onListenerCreated.emit(created);

    return created;
  }

  async updateListener(listener: EventListener) {
    const updated = {
      ...listener,
      updatedAt: new Date(),
    };
    await this.listenerTable()
      .where({
        id: listener.id,
      })
      .update(updated);

    return updated;
  }

  async deleteListener(listener: EventListener) {
    await this.listenerTable().where({ id: listener.id }).delete();
  }
}

export class EventService {
  constructor(readonly table: Factory<EventTable>) {}

  async create(
    eventListener: EventListener,
    event: ethers.Event,
    network: string,
    from: string,
    timestamp: number
  ) {
    const args = Object.entries(event.args || {}).reduce((res, [k, v]) => {
      if (!isNaN(parseInt(k, 10))) return res;

      return {
        ...res,
        [k]: v.toString(),
      };
    }, {});

    const created = {
      id: uuid(),
      eventListener: eventListener.id,
      address: event.address.toLowerCase(),
      blockNumber: event.blockNumber,
      blockHash: event.blockHash.toLowerCase(),
      transactionIndex: event.transactionIndex,
      transactionHash: event.transactionHash.toLowerCase(),
      logIndex: event.logIndex,
      args: JSON.stringify(args, null, 4),
      network,
      from: from.toLowerCase(),
      createdAt: dayjs.unix(timestamp).toDate(),
    };
    await this.table().insert(created);

    return created;
  }
}
