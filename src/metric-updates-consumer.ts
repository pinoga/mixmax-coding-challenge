import {
  DynamoDBClient,
  UpdateItemCommand,
  UpdateItemCommandOutput,
} from "@aws-sdk/client-dynamodb";
import { SQSBatchResponse, SQSEvent } from "aws-lambda";
import {
  MetricUpdatesMessage,
  MetricUpdatesMessageSchema,
} from "./events/metric-updates-event";
import { Logger } from "./logger/logger";
import { DynamoDBMapper } from "./mappers/dynamodb.mapper";

interface UpdateItemRequest {
  sk: string;
  pk: string;
  inc: number;
  messageIds: string[];
}

type ItemIDToUpdateItemRequestMap = Record<string, UpdateItemRequest>;

export const main = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  const client = new DynamoDBClient({});
  const logger = Logger.instance();
  const tableName = `feature-usage-${process.env.ENV || "local"}`;

  const batchItemFailedMessageIDs = new Set<string>();

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

  await Promise.all(
    Object.values(batchItemRequests).map<
      Promise<UpdateItemCommandOutput | void>
    >(({ pk, sk, inc, messageIds }) => {
      return client
        .send(
          new UpdateItemCommand({
            TableName: tableName,
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
        });
    }),
  );

  return {
    batchItemFailures: [...batchItemFailedMessageIDs].map((itemIdentifier) => ({
      itemIdentifier,
    })),
  };
};

module.exports = { main };
