import { Column, CreateDateColumn, Entity, PrimaryColumn } from 'typeorm';

@Entity('notification_logs')
export class NotificationLog {
  @PrimaryColumn()
  eventId!: string;

  @Column()
  activationId!: string;

  @Column()
  customerId!: string;

  @Column()
  eventType!: string;

  @Column({ type: 'varchar', default: 'PENDING' })
  status!: 'PENDING' | 'SENT';

  @CreateDateColumn()
  createdAt!: Date;
}
