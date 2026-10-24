import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { TypeOrmModule } from '@nestjs/typeorm';
import { OutboxDispatcher, OutboxEvent, ProcessedEvent } from '@poc/reliability';
import { BillingAccount } from './billing-account.entity';
import { BillingController } from './billing.controller';

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.DB_HOST ?? 'localhost',
      port: Number(process.env.DB_PORT ?? 5433),
      username: process.env.DB_USER ?? 'poc',
      password: process.env.DB_PASSWORD ?? 'poc',
      database: process.env.DB_NAME ?? 'billing_db',
      autoLoadEntities: true,
      synchronize: true,
    }),
    TypeOrmModule.forFeature([BillingAccount, OutboxEvent, ProcessedEvent]),
    ClientsModule.register([
      {
        name: 'KAFKA_CLIENT',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'billing-service-producer',
            brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
          },
          producerOnlyMode: true,
        },
      },
    ]),
  ],
  controllers: [BillingController],
  providers: [OutboxDispatcher],
})
export class AppModule {}