import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { TypeOrmModule } from '@nestjs/typeorm';
import { OutboxDispatcher, OutboxEvent, ProcessedEvent } from '@poc/reliability';
import { Activation } from './activation.entity';
import { ActivationEventsController } from './activation-events.controller';
import { ActivationGateway } from './activation.gateway';
import { ActivationsController } from './activations.controller';
import { ActivationsService } from './activations.service';

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.DB_HOST ?? 'localhost',
      port: Number(process.env.DB_PORT ?? 5433),
      username: process.env.DB_USER ?? 'poc',
      password: process.env.DB_PASSWORD ?? 'poc',
      database: process.env.DB_NAME ?? 'activation_db',
      autoLoadEntities: true,
      synchronize: true,
    }),
    TypeOrmModule.forFeature([Activation, OutboxEvent, ProcessedEvent]),
    ClientsModule.register([
      {
        name: 'KAFKA_CLIENT',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'activation-api',
            brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
          },
          producerOnlyMode: true,
        },
      },
    ]),
  ],
  controllers: [ActivationsController, ActivationEventsController],
  providers: [ActivationsService, ActivationGateway, OutboxDispatcher],
})
export class ActivationsModule {}