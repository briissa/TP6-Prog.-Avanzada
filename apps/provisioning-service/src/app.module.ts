import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { TypeOrmModule } from '@nestjs/typeorm';
import { OutboxDispatcher, OutboxEvent, ProcessedEvent } from '@poc/reliability';
import { ProvisioningOrder } from './provisioning-order.entity';
import { ProvisioningController } from './provisioning.controller';

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.DB_HOST ?? 'localhost',
      port: Number(process.env.DB_PORT ?? 5433),
      username: process.env.DB_USER ?? 'poc',
      password: process.env.DB_PASSWORD ?? 'poc',
      database: process.env.DB_NAME ?? 'provisioning_db',
      autoLoadEntities: true,
      synchronize: true,
    }),
    TypeOrmModule.forFeature([ProvisioningOrder, OutboxEvent, ProcessedEvent]),
    ClientsModule.register([
      {
        name: 'KAFKA_CLIENT',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'provisioning-service-producer',
            brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
          },
          producerOnlyMode: true,
        },
      },
    ]),
  ],
  controllers: [ProvisioningController],
  providers: [OutboxDispatcher],
})
export class AppModule {}