import {
  DynamoDBClient,
  UpdateItemCommand,
  UpdateItemCommandInput,
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
}

type ItemIDToUpdateItemRequestMap = Record<string, UpdateItemRequest>;
type UpdateItemRequestPromises = Promise<UpdateItemRequest>[];

export const main = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  const client = new DynamoDBClient({});
  const logger = Logger.instance();
  const tableName = `feature-usage-${process.env.ENV || "local"}`;
  const batchChunkSize = Number(process.env.BATCH_CHUNK_SIZE) || 100;

  const batchItemFailedMessageIDs = new Set<string>();

  const baseUpdateItemCommandInput: UpdateItemCommandInput = {
    TableName: tableName,
    Key: {
      pk: { S: "" },
      sk: { S: "" },
    },
    UpdateExpression: "ADD #count :inc",
    ExpressionAttributeNames: { "#count": "count" },
    ExpressionAttributeValues: { ":inc": { N: "" } },
  };

  let parsedMessageBody: MetricUpdatesMessageSchema;
  let itemID: string;

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
        } else {
          acc[item.id] = {
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

  // await Promise.all(batchItemRequests);

  for (const record of event.Records) {
    const body = JSON.parse(record.body);

    if (body.workspaceId) {
      await client.send(
        new UpdateItemCommand({
          TableName: tableName,
          Key: {
            pk: { S: `WSP#${body.workspaceId}#MET#${body.metricId}` },
            sk: { S: `H#${body.date}` },
          },
          UpdateExpression: "ADD #count :inc",
          ExpressionAttributeNames: { "#count": "count" },
          ExpressionAttributeValues: { ":inc": { N: body.count.toString() } },
        }),
      );

      await client.send(
        new UpdateItemCommand({
          TableName: tableName,
          Key: {
            pk: { S: `WSP#${body.workspaceId}#MET#${body.metricId}` },
            sk: { S: `D#${body.date.substring(0, 10)}` },
          },
          UpdateExpression: "ADD #count :inc",
          ExpressionAttributeNames: { "#count": "count" },
          ExpressionAttributeValues: { ":inc": { N: body.count.toString() } },
        }),
      );
    }

    if (body.userId && body.workspaceId) {
      await client.send(
        new UpdateItemCommand({
          TableName: tableName,
          Key: {
            pk: { S: `USR#${body.userId}#MET#${body.metricId}` },
            sk: { S: `H#${body.date}` },
          },
          UpdateExpression: "ADD #count :inc",
          ExpressionAttributeNames: { "#count": "count" },
          ExpressionAttributeValues: { ":inc": { N: body.count.toString() } },
        }),
      );

      await client.send(
        new UpdateItemCommand({
          TableName: tableName,
          Key: {
            pk: { S: `USR#${body.userId}#MET#${body.metricId}` },
            sk: { S: `D#${body.date.substring(0, 10)}` },
          },
          UpdateExpression: "ADD #count :inc",
          ExpressionAttributeNames: { "#count": "count" },
          ExpressionAttributeValues: { ":inc": { N: body.count.toString() } },
        }),
      );
    }
  }

  return {
    batchItemFailures: [...batchItemFailedMessageIDs].map((itemIdentifier) => ({
      itemIdentifier,
    })),
  };
};

module.exports = { main };
