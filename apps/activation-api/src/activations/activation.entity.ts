import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryColumn,
  UpdateDateColumn,
} from 'typeorm';
import type { SimulateFailure } from '@poc/contracts';

export type ActivationStatus = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';

@Entity('activations')
export class Activation {
  @PrimaryColumn()
  id!: string;

  @Index()
  @Column()
  customerId!: string;

  @Column()
  planId!: string;

  @Column({ type: 'varchar', default: 'PENDING' })
  status!: ActivationStatus;

  @Column({ type: 'varchar', default: 'none' })
  simulateFailure!: SimulateFailure;

  @Column({ type: 'jsonb', default: () => "'{}'" })
  steps!: Record<string, { status: string; at: string }>;

  @Column({ type: 'jsonb', default: () => "'[]'" })
  history!: Array<{ eventId: string; eventType: string; at: string }>;

  @CreateDateColumn()
  createdAt!: Date;

  @UpdateDateColumn()
  updatedAt!: Date;
}