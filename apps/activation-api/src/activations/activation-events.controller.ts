import { Controller, Inject, Logger } from '@nestjs/common';
import { ClientKafka, EventPattern, Payload } from '@nestjs/microservices';
import { dlqOf, TOPICS } from '@poc/contracts';
import type { AnyEvent } from '@poc/contracts';
import { processWithRetries } from '@poc/reliability';
import { lastValueFrom } from 'rxjs';
import { ActivationGateway } from './activation.gateway';
import { ActivationsService } from './activations.service';

type ActivationResult = Extract<
  AnyEvent,
  | { eventType: 'BillingAccountCreated' }
  | { eventType: 'BillingFailed' }
  | { eventType: 'ProvisioningCompleted' }
  | { eventType: 'ProvisioningFailed' }
>;

@Controller()
export class ActivationEventsController {
  private readonly logger = new Logger(ActivationEventsController.name);

  constructor(
    private readonly activations: ActivationsService,
    private readonly gateway: ActivationGateway,
    @Inject('KAFKA_CLIENT') private readonly kafka: ClientKafka,
  ) {}

  @EventPattern(TOPICS.BILLING_EVENTS)
  async onBillingEvent(
    @Payload()
    event: Extract<
      AnyEvent,
      { eventType: 'BillingAccountCreated' | 'BillingFailed' }
    >,
  ) {
    await this.handle(event, TOPICS.BILLING_EVENTS);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async onProvisioningEvent(
    @Payload()
    event: Extract<
      AnyEvent,
      { eventType: 'ProvisioningCompleted' | 'ProvisioningFailed' }
    >,
  ) {
    await this.handle(event, TOPICS.PROVISIONING_EVENTS);
  }

  private async handle(event: ActivationResult, topic: string) {
    await processWithRetries(
      topic,
      event,
      async () => {
        const activation = await this.activations.recordResult(
          event,
          topic,
        );
        if (activation) this.gateway.publish(activation);
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
