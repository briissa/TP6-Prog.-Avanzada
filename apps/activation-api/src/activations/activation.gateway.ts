import { WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import { Server } from 'socket.io';
import { Activation } from './activation.entity';

@WebSocketGateway({ cors: { origin: '*' } })
export class ActivationGateway {
  @WebSocketServer()
  private server!: Server;

  publish(activation: Activation) {
    this.server.emit('activation.updated', activation);
  }
}
