import { Controller, Inject, Logger } from '@nestjs/common';
import { ClientKafka, EventPattern, Payload } from '@nestjs/microservices';
import { InjectDataSource } from '@nestjs/typeorm';
import { randomUUID } from 'crypto';
import { lastValueFrom } from 'rxjs';
import { DataSource } from 'typeorm';
import { createEvent, TOPICS } from '@poc/contracts';
import { dlqOf } from '@poc/contracts';
import type { AnyEvent } from '@poc/contracts';
import { markEventProcessed, OutboxEvent, processWithRetries } from '@poc/reliability';
import { ProvisioningOrder } from './provisioning-order.entity';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

@Controller()
export class ProvisioningController {
  private readonly logger = new Logger(ProvisioningController.name);

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    @Inject('KAFKA_CLIENT') private readonly kafka: ClientKafka,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async onActivationRequested(
    @Payload() event: Extract<AnyEvent, { eventType: 'ActivationRequested' }>,
  ) {
    await processWithRetries(
      TOPICS.ACTIVATION_REQUESTED,
      event,
      async () => {
        await sleep(1000 + Math.random() * 2000);
        await this.dataSource.transaction(async (manager) => {
          if (
            !(await markEventProcessed(manager, event.eventId, TOPICS.ACTIVATION_REQUESTED))
          ) {
            return;
          }
          const orders = manager.getRepository(ProvisioningOrder);
          const existing = await orders.findOneBy({ activationId: event.correlationId });
          if (existing) return;

          if (event.payload.simulateFailure === 'provisioning') {
            const failed = createEvent({
              eventType: 'ProvisioningFailed',
              correlationId: event.correlationId,
              customerId: event.customerId,
              source: 'provisioning-service',
              payload: { reason: 'Fallo de aprovisionamiento simulado' },
            });
            await manager.getRepository(OutboxEvent).save({
              id: failed.eventId,
              topic: TOPICS.PROVISIONING_EVENTS,
              messageKey: event.customerId,
              payload: failed,
            });
            return;
          }

          const orderId = `ord-${randomUUID().slice(0, 8)}`;
          await orders.save({
            id: orderId,
            activationId: event.correlationId,
            customerId: event.customerId,
            status: 'COMPLETED',
          });
          const completed = createEvent({
            eventType: 'ProvisioningCompleted',
            correlationId: event.correlationId,
            customerId: event.customerId,
            source: 'provisioning-service',
            payload: { orderId },
          });
          await manager.getRepository(OutboxEvent).save({
            id: completed.eventId,
            topic: TOPICS.PROVISIONING_EVENTS,
            messageKey: event.customerId,
            payload: completed,
          });
        });
      },
      async (failedEvent, reason) => {
        await this.publishDlq(failedEvent, reason);
      },
      (message, error) => this.logger.error(message, error.stack),
    );
  }

  private async publishDlq(event: AnyEvent, reason: string) {
    await lastValueFrom(
      this.kafka.emit(dlqOf(TOPICS.ACTIVATION_REQUESTED), {
        key: event.customerId,
        value: event,
        headers: { 'error-message': reason },
      }),
    );
  }
}