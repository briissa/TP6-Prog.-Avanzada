import { Column, CreateDateColumn, Entity, PrimaryColumn } from 'typeorm';
import type { AnyEvent } from '@poc/contracts';

@Entity('recorded_events')
export class RecordedEvent {
  @PrimaryColumn()
  eventId!: string;

  @Column()
  topic!: string;

  @Column()
  correlationId!: string;

  @Column()
  eventType!: string;

  @Column({ type: 'jsonb' })
  event!: AnyEvent;

  @CreateDateColumn()
  recordedAt!: Date;
}
