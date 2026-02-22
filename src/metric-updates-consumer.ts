import { UpdateItemCommand } from "@aws-sdk/client-dynamodb";
import { SQSBatchResponse, SQSEvent } from "aws-lambda";
import {
  MetricUpdatesMessage,
  MetricUpdatesMessageSchema,
} from "./dto/metric-updates-event";
import { DynamoDBClientFactory } from "./dynamodb/dynamodb-client";
import { Logger } from "./logger/logger";
import { DynamoDBMapper } from "./mappers/dynamodb.mapper";
import { concurrently } from "./utils/concurrently";

interface UpdateItemRequest {
  sk: string;
  pk: string;
  inc: number;
  messageIds: string[];
}

type ItemIDToUpdateItemRequestMap = Record<string, UpdateItemRequest>;
const dynamoDbTableName = process.env.DYNAMODB_TABLE_NAME;
const concurrency = Number(process.env.DYNAMODB_WRITE_CONCURRENCY) || 300;
const client = DynamoDBClientFactory.create({
  requestHandler: {
    requestTimeout:
      Number(process.env.DYNAMODB_CLIENT_REQUEST_TIMEOUT_MS) || 3000,
    httpsAgent: { maxSockets: concurrency },
  },
});
const logger = Logger.instance();

export const main = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  const batchItemFailedMessageIDs = new Set<string>();

  // aggregating increments for each DynamoDB item to save round-trips
  const batchItemRequests = event.Records.reduce<ItemIDToUpdateItemRequestMap>(
    (acc, record) => {
      let message: MetricUpdatesMessageSchema;
      try {
        message = MetricUpdatesMessage.validate(record.body);
      } catch (error) {
        logger.error({
          message: "Invalid MetricUpdatesMessage, skipping",
          meta: { error },
        });
        return acc;
      }
      const items = DynamoDBMapper.eventMessageToItems(message);

      for (const item of items) {
        let updateItemRequest: UpdateItemRequest | undefined = acc[item.id];
        if (updateItemRequest) {
          updateItemRequest.inc += message.count;
          updateItemRequest.messageIds.push(record.messageId);
        } else {
          acc[item.id] = {
            messageIds: [record.messageId],
            sk: item.sk,
            pk: item.pk,
            inc: message.count,
          };
        }
      }

      return acc;
    },
    {},
  );

  // Signal the event loop that we no longer need this in memory
  // @ts-expect-error
  event = null;

  // Why not a plain Promise.all?
  // For smaller batches, it may not make much difference, but the Node.js can only
  // process so much I/O in parallel, so we may want to increase the SQS batch size independently
  // (to gain performance on deduplication) from the concurrency limit
  await concurrently(
    Object.values(batchItemRequests).map(
      ({ pk, sk, inc, messageIds }) =>
        () =>
          client
            .send(
              new UpdateItemCommand({
                TableName: dynamoDbTableName,
                Key: {
                  pk: { S: pk },
                  sk: { S: sk },
                },
                UpdateExpression: "ADD #count :inc",
                ExpressionAttributeNames: { "#count": "count" },
                ExpressionAttributeValues: { ":inc": { N: inc.toString() } },
              }),
            )
            .catch((error) => {
              messageIds.forEach((messageId) => {
                batchItemFailedMessageIDs.add(messageId);
              });
              logger.error({
                message: "Error updating metrics for DynamoDB item. Requeueing",
                meta: {
                  messageIds,
                  error,
                  pk,
                  sk,
                  inc,
                },
              });
            }),
    ),
    concurrency,
  );

  return {
    batchItemFailures: [...batchItemFailedMessageIDs].map((itemIdentifier) => ({
      itemIdentifier,
    })),
  };
};

module.exports = { main };
