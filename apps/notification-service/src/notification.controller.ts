import { Controller, Inject, Logger } from '@nestjs/common';
import { ClientKafka, EventPattern, Payload } from '@nestjs/microservices';
import { InjectDataSource } from '@nestjs/typeorm';
import { dlqOf, TOPICS } from '@poc/contracts';
import type { AnyEvent } from '@poc/contracts';
import { markEventProcessed, processWithRetries } from '@poc/reliability';
import { InjectRepository } from '@nestjs/typeorm';
import nodemailer from 'nodemailer';
import { lastValueFrom } from 'rxjs';
import { DataSource, Repository } from 'typeorm';
import { NotificationLog } from './notification-log.entity';

@Controller()
export class NotificationController {
  private readonly logger = new Logger(NotificationController.name);
  private readonly mailer = nodemailer.createTransport({
    host: process.env.SMTP_HOST ?? 'localhost',
    port: Number(process.env.SMTP_PORT ?? 1025),
    secure: false,
  });

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectRepository(NotificationLog)
    private readonly notifications: Repository<NotificationLog>,
    @Inject('KAFKA_CLIENT') private readonly kafka: ClientKafka,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async onActivationEvent(@Payload() event: AnyEvent) {
    await processWithRetries(
      TOPICS.ACTIVATION_EVENTS,
      event,
      async () => {
        const notification = await this.dataSource.transaction(async (manager) => {
          const isNew = await markEventProcessed(
            manager,
            event.eventId,
            TOPICS.ACTIVATION_EVENTS,
          );
          if (
            isNew &&
            (event.eventType === 'ActivationCompleted' ||
              event.eventType === 'ActivationFailed')
          ) {
            await manager.getRepository(NotificationLog).save({
              eventId: event.eventId,
              activationId: event.correlationId,
              customerId: event.customerId,
              eventType: event.eventType,
              status: 'PENDING',
            });
          }
          return manager
            .getRepository(NotificationLog)
            .findOneBy({ eventId: event.eventId });
        });
        if (!notification || notification.status === 'SENT') return;

        const success = notification.eventType === 'ActivationCompleted';
        await this.mailer.sendMail({
          from: process.env.MAIL_FROM ?? 'activation@poc.local',
          to: `${notification.customerId}@example.com`,
          subject: success ? 'Tu servicio está activo' : 'No pudimos activar tu servicio',
          text: success
            ? `La activación ${notification.activationId} se completó correctamente.`
            : `La activación ${notification.activationId} falló y fue compensada.`,
        });
        await this.notifications.update(
          { eventId: notification.eventId },
          { status: 'SENT' },
        );
      },
      async (failedEvent, reason) => {
        await lastValueFrom(
          this.kafka.emit(dlqOf(TOPICS.ACTIVATION_EVENTS), {
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
