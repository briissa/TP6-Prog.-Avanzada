import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { lastValueFrom } from 'rxjs';
import { OutboxEvent } from './entities';

@Injectable()
export class OutboxDispatcher implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxDispatcher.name);
  private stopped = false;

  constructor(
    @InjectRepository(OutboxEvent)
    private readonly outbox: Repository<OutboxEvent>,
    @Inject('KAFKA_CLIENT') private readonly kafka: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafka.connect();
    void this.dispatchLoop();
  }

  onModuleDestroy() {
    this.stopped = true;
  }

  private async dispatchLoop() {
    while (!this.stopped) {
      try {
        const count = await this.outbox.manager.transaction(async (manager) => {
          const events = await manager
            .getRepository(OutboxEvent)
            .createQueryBuilder('event')
            .where('event.publishedAt IS NULL')
            .orderBy('event.createdAt', 'ASC')
            .take(50)
            .setLock('pessimistic_write')
            .setOnLocked('skip_locked')
            .getMany();

          for (const event of events) {
            await lastValueFrom(
              this.kafka.emit(event.topic, {
                key: event.messageKey,
                value: event.payload,
                headers: event.headers,
              }),
            );
            await manager.getRepository(OutboxEvent).update(
              { id: event.id },
              { publishedAt: new Date() },
            );
          }
          return events.length;
        });
        if (count === 0) {
          await new Promise((resolve) => setTimeout(resolve, 500));
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        this.logger.error(`Outbox dispatch failed: ${message}`);
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }
}
