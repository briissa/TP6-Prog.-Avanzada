import { Column, CreateDateColumn, Entity, Index, PrimaryColumn } from 'typeorm';

@Entity('billing_accounts')
export class BillingAccount {
  @PrimaryColumn()
  id!: string;

  @Index({ unique: true })
  @Column()
  activationId!: string;

  @Column()
  customerId!: string;

  @Column({ type: 'varchar', default: 'CREATED' })
  status!: 'CREATED' | 'CANCELLED';

  @CreateDateColumn()
  createdAt!: Date;
}