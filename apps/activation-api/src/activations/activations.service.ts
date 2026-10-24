import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectDataSource, InjectRepository } from '@nestjs/typeorm';
import { randomUUID } from 'crypto';
import { DataSource, Repository } from 'typeorm';
import {
  ActivationFailedPayload,
  createEvent,
  TOPICS,
} from '@poc/contracts';
import type { AnyEvent } from '@poc/contracts';
import { markEventProcessed, OutboxEvent } from '@poc/reliability';
import { Activation } from './activation.entity';
import { CreateActivationDto } from './dto/create-activation.dto';

type ActivationResult = Extract<
  AnyEvent,
  | { eventType: 'BillingAccountCreated' }
  | { eventType: 'BillingFailed' }
  | { eventType: 'ProvisioningCompleted' }
  | { eventType: 'ProvisioningFailed' }
>;

@Injectable()
export class ActivationsService {
  constructor(
    @InjectRepository(Activation) private readonly repo: Repository<Activation>,
    @InjectDataSource() private readonly dataSource: DataSource,
  ) {}

  async create(dto: CreateActivationDto) {
    const id = `act-${randomUUID().slice(0, 8)}`;

    const event = createEvent({
      eventType: 'ActivationRequested',
      correlationId: id,
      customerId: dto.customerId,
      source: 'activation-api',
      payload: {
        planId: dto.planId,
        channel: dto.channel ?? 'web',
        simulateFailure: dto.simulateFailure ?? 'none',
      },
    });

    // 1) Guardar la activación en PENDING
    await this.dataSource.transaction(async (manager) => {
      await manager.getRepository(Activation).save({
        id,
        customerId: dto.customerId,
        planId: dto.planId,
        status: 'PENDING',
        simulateFailure: dto.simulateFailure ?? 'none',
        steps: {},
        history: [{ eventId: event.eventId, eventType: event.eventType, at: event.occurredAt }],
      });
      await manager.getRepository(OutboxEvent).save({
        id: event.eventId,
        topic: TOPICS.ACTIVATION_REQUESTED,
        messageKey: dto.customerId,
        payload: event,
      });
    });

    return { activationId: id, status: 'PENDING' };
  }

  async findOne(id: string) {
    const activation = await this.repo.findOneBy({ id });
    if (!activation) {
      throw new NotFoundException(`Activación ${id} no encontrada`);
    }
    return activation;
  }

  async recordResult(event: ActivationResult, topic: string) {
    return this.dataSource.transaction(async (manager) => {
      if (!(await markEventProcessed(manager, event.eventId, topic))) {
        const existing = await manager
          .getRepository(Activation)
          .findOneBy({ id: event.correlationId });
        if (!existing) {
          throw new NotFoundException(`Activación ${event.correlationId} no encontrada`);
        }
        return existing;
      }

      const activations = manager.getRepository(Activation);
      const activation = await activations.findOneBy({ id: event.correlationId });
      if (!activation) {
        throw new NotFoundException(`Activación ${event.correlationId} no encontrada`);
      }

      activation.history.push({
        eventId: event.eventId,
        eventType: event.eventType,
        at: event.occurredAt,
      });

      if (activation.status === 'ACTIVE' || activation.status === 'FAILED') {
        await activations.save(activation);
        return activation;
      }

      let terminalEvent: AnyEvent | undefined;
      if (event.eventType === 'BillingFailed' || event.eventType === 'ProvisioningFailed') {
        const failedStep = event.eventType === 'BillingFailed' ? 'billing' : 'provisioning';
        activation.steps[failedStep] = { status: 'FAILED', at: event.occurredAt };
        activation.status = 'FAILED';
        const payload: ActivationFailedPayload = {
          reason: event.payload.reason,
          failedStep,
        };
        terminalEvent = createEvent({
          eventType: 'ActivationFailed',
          correlationId: activation.id,
          customerId: activation.customerId,
          source: 'activation-api',
          payload,
        });
      } else {
        const step = event.eventType === 'BillingAccountCreated' ? 'billing' : 'provisioning';
        activation.steps[step] = {
          status: event.eventType === 'BillingAccountCreated' ? 'CREATED' : 'COMPLETED',
          at: event.occurredAt,
        };
        if (activation.steps.billing && activation.steps.provisioning) {
          activation.status = 'ACTIVE';
          terminalEvent = createEvent({
            eventType: 'ActivationCompleted',
            correlationId: activation.id,
            customerId: activation.customerId,
            source: 'activation-api',
            payload: {},
          });
        } else {
          activation.status = 'IN_PROGRESS';
        }
      }

      if (terminalEvent) {
        activation.history.push({
          eventId: terminalEvent.eventId,
          eventType: terminalEvent.eventType,
          at: terminalEvent.occurredAt,
        });
      }
      await activations.save(activation);

      if (terminalEvent) {
        await manager.getRepository(OutboxEvent).save({
          id: terminalEvent.eventId,
          topic: TOPICS.ACTIVATION_EVENTS,
          messageKey: activation.customerId,
          payload: terminalEvent,
        });
      }
      return activation;
    });
  }
}