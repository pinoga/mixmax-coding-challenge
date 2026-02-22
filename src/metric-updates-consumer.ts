import { SQSBatchResponse, SQSEvent } from "aws-lambda";
import { DynamoDBClientFactory } from "./dynamodb/dynamodb-client";
import { MetricsRepository } from "./dynamodb/dynamodb-repository";
import { MetricsService } from "./metric-service";

const client = DynamoDBClientFactory.create({
  requestHandler: {
    requestTimeout:
      Number(process.env.DYNAMODB_CLIENT_REQUEST_TIMEOUT_MS) || 3000,
    httpsAgent: { maxSockets: MetricsService.batchUpdateConcurrency },
  },
});
const metricsService = new MetricsService(new MetricsRepository(client));

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

module.exports = { main };
