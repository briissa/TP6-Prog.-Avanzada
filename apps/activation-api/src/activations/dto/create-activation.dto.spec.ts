import { validate } from 'class-validator';
import { CreateActivationDto } from './create-activation.dto';

describe('CreateActivationDto', () => {
  it('accepts the supported activation fields', async () => {
    const dto = Object.assign(new CreateActivationDto(), {
      customerId: 'cliente-1',
      planId: 'plan-1',
      simulateFailure: 'provisioning',
      channel: 'web',
    });

    await expect(validate(dto)).resolves.toHaveLength(0);
  });

  it('rejects unsupported failure modes and strips unknown properties', async () => {
    const dto = Object.assign(new CreateActivationDto(), {
      customerId: 'cliente-1',
      planId: 'plan-1',
      simulateFailure: 'database',
      unexpected: true,
    });

    const errors = await validate(dto, { whitelist: true });
    expect(errors.map(({ property }) => property)).toContain('simulateFailure');
    expect(dto).not.toHaveProperty('unexpected');
  });
});
