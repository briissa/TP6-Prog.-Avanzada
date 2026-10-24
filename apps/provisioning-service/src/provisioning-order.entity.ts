import { Column, CreateDateColumn, Entity, Index, PrimaryColumn } from 'typeorm';

@Entity('provisioning_orders')
export class ProvisioningOrder {
  @PrimaryColumn()
  id!: string;

  @Index({ unique: true })
  @Column()
  activationId!: string;

  @Column()
  customerId!: string;

  @Column({ type: 'varchar', default: 'COMPLETED' })
  status!: 'COMPLETED' | 'FAILED';

  @CreateDateColumn()
  createdAt!: Date;
}