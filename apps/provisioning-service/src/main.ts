import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'provisioning-service',
        brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
      },
      consumer: { groupId: 'provisioning-svc' },
    },
  });
  await app.listen();
}
bootstrap();