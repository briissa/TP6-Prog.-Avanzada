import { Column, CreateDateColumn, Entity, Index, PrimaryColumn } from 'typeorm';

@Entity('processed_events')
export class ProcessedEvent {
  @PrimaryColumn()
  eventId!: string;

  @Column()
  topic!: string;

  @CreateDateColumn()
  processedAt!: Date;
}

@Entity('outbox_events')
@Index(['publishedAt', 'createdAt'])
export class OutboxEvent {
  @PrimaryColumn()
  id!: string;

  @Column()
  topic!: string;

  @Column()
  messageKey!: string;

  @Column({ type: 'jsonb' })
  payload!: unknown;

  @Column({ type: 'jsonb', nullable: true })
  headers?: Record<string, string>;

  @Column({ type: 'timestamptz', nullable: true })
  publishedAt!: Date | null;

  @CreateDateColumn()
  createdAt!: Date;
}
