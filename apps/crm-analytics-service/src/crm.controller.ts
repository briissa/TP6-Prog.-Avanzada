import { Controller, Inject, Logger } from '@nestjs/common';
import { ClientKafka, EventPattern, Payload } from '@nestjs/microservices';
import { InjectDataSource } from '@nestjs/typeorm';
import { dlqOf, TOPICS } from '@poc/contracts';
import type { AnyEvent } from '@poc/contracts';
import { markEventProcessed, processWithRetries } from '@poc/reliability';
import { lastValueFrom } from 'rxjs';
import { DataSource } from 'typeorm';
import { RecordedEvent } from './recorded-event.entity';

@Controller()
export class CrmController {
  private readonly logger = new Logger(CrmController.name);

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    @Inject('KAFKA_CLIENT') private readonly kafka: ClientKafka,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onActivationRequested(@Payload() event: AnyEvent) {
    return this.record(TOPICS.ACTIVATION_REQUESTED, event);
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  onBillingEvent(@Payload() event: AnyEvent) {
    return this.record(TOPICS.BILLING_EVENTS, event);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  onProvisioningEvent(@Payload() event: AnyEvent) {
    return this.record(TOPICS.PROVISIONING_EVENTS, event);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Payload() event: AnyEvent) {
    return this.record(TOPICS.ACTIVATION_EVENTS, event);
  }

  private async record(topic: string, event: AnyEvent) {
    await processWithRetries(
      topic,
      event,
      async () => {
        await this.dataSource.transaction(async (manager) => {
          if (!(await markEventProcessed(manager, event.eventId, topic))) return;
          await manager.getRepository(RecordedEvent).save({
            eventId: event.eventId,
            topic,
            correlationId: event.correlationId,
            eventType: event.eventType,
            event,
          });
        });
      },
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
