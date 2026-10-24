import { EntityManager } from 'typeorm';
import { ProcessedEvent } from './entities';

export async function markEventProcessed(
  manager: EntityManager,
  eventId: string,
  topic: string,
) {
  const result = await manager
    .createQueryBuilder()
    .insert()
    .into(ProcessedEvent)
    .values({ eventId, topic })
    .orIgnore()
    .returning(['eventId'])
    .execute();
  return result.raw.length > 0;
}

export async function processWithRetries<T>(
  topic: string,
  event: T,
  handler: () => Promise<void>,
  publishToDlq: (event: T, reason: string) => Promise<void>,
  logError: (message: string, error: Error) => void,
) {
  const retryDelays = [1000, 2000, 4000];
  let lastError = new Error('Unknown consumer error');

  for (let attempt = 0; attempt <= retryDelays.length; attempt += 1) {
    try {
      await handler();
      return;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      logError(
        `${topic} attempt ${attempt + 1}/${retryDelays.length + 1} failed`,
        lastError,
      );
      if (attempt < retryDelays.length) {
        await new Promise((resolve) => setTimeout(resolve, retryDelays[attempt]));
      }
    }
  }

  await publishToDlq(event, lastError.message);
}
