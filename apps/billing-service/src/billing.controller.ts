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
import { BillingAccount } from './billing-account.entity';

@Controller()
export class BillingController {
  private readonly logger = new Logger(BillingController.name);

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    @Inject('KAFKA_CLIENT') private readonly kafka: ClientKafka,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async onActivationRequested(
    @Payload() event: Extract<AnyEvent, { eventType: 'ActivationRequested' }>,
  ) {
    await this.consume(TOPICS.ACTIVATION_REQUESTED, event, async () => {
      await this.dataSource.transaction(async (manager) => {
        if (!(await markEventProcessed(manager, event.eventId, TOPICS.ACTIVATION_REQUESTED))) {
          return;
        }
        const accounts = manager.getRepository(BillingAccount);
        const existing = await accounts.findOneBy({ activationId: event.correlationId });
        if (existing) return;

        if (event.payload.simulateFailure === 'billing') {
          const failed = createEvent({
            eventType: 'BillingFailed',
            correlationId: event.correlationId,
            customerId: event.customerId,
            source: 'billing-service',
            payload: { reason: 'Fallo de facturación simulado' },
          });
          await manager.getRepository(OutboxEvent).save({
            id: failed.eventId,
            topic: TOPICS.BILLING_EVENTS,
            messageKey: event.customerId,
            payload: failed,
          });
          return;
        }

        const accountId = `acc-${randomUUID().slice(0, 8)}`;
        await accounts.save({
          id: accountId,
          activationId: event.correlationId,
          customerId: event.customerId,
          status: 'CREATED',
        });
        const created = createEvent({
          eventType: 'BillingAccountCreated',
          correlationId: event.correlationId,
          customerId: event.customerId,
          source: 'billing-service',
          payload: { accountId },
        });
        await manager.getRepository(OutboxEvent).save({
          id: created.eventId,
          topic: TOPICS.BILLING_EVENTS,
          messageKey: event.customerId,
          payload: created,
        });
      });
    });
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async onActivationEvent(@Payload() event: AnyEvent) {
    await this.consume(TOPICS.ACTIVATION_EVENTS, event, async () => {
      await this.dataSource.transaction(async (manager) => {
        if (!(await markEventProcessed(manager, event.eventId, TOPICS.ACTIVATION_EVENTS))) {
          return;
        }
        if (event.eventType !== 'ActivationFailed') return;

        const accounts = manager.getRepository(BillingAccount);
        const existing = await accounts.findOneBy({ activationId: event.correlationId });
        if (existing) {
          existing.status = 'CANCELLED';
          await accounts.save(existing);
        } else {
          await accounts.save({
            id: `cancel-${event.correlationId}`,
            activationId: event.correlationId,
            customerId: event.customerId,
            status: 'CANCELLED',
          });
        }
      });
    });
  }

  private async consume(
    topic: string,
    event: AnyEvent,
    handler: () => Promise<void>,
  ) {
    await processWithRetries(
      topic,
      event,
      handler,
      async (failedEvent, reason) => {
        await lastValueFrom(
          this.kafka.emit(dlqOf(topic), {
            key: failedEvent.customerId,
            value: failedEvent,
            headers: { 'error-message': reason },
          }),
        );
      },
      (message, error) => this.logger.error(message, error.stack),
    );
  }
}