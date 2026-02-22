import { SQSBatchResponse, SQSEvent } from "aws-lambda";
import { MetricsService } from "./metric-service";

const metricsService = new MetricsService();

export const main = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  // aggregating increments for each DynamoDB item to save round-trips
  const batchItemRequests = metricsService.aggregateMetricUpdateRecords(
    event.Records,
  );

  /* eslint-disable no-useless-assignment */
  // @ts-expect-error Signal the event loop that we no longer need this in memory
  event = null;
  /* eslint-enable no-useless-assignment */

  const batchItemFailedMessageIDs =
    await metricsService.batchIncrementMetrics(batchItemRequests);

  return {
    batchItemFailures: [...batchItemFailedMessageIDs].map((itemIdentifier) => ({
      itemIdentifier,
    })),
  };
};
