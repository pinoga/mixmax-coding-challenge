import {
  DynamoDBClient,
  UpdateItemCommand,
  UpdateItemCommandInput,
} from "@aws-sdk/client-dynamodb";
import { SQSBatchResponse, SQSEvent } from "aws-lambda";
import { EventMessageBody } from "./types/event";

interface UpdateItemRequest {
  sk: string;
  pk: string;
  inc: number;
}

type ItemIDToUpdateItemRequestMap = Record<string, UpdateItemRequest>;
type UpdateItemRequestPromises = Promise<UpdateItemRequest>[];

export const main = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  const client = new DynamoDBClient({});
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

  let parsedMessageBody: EventMessageBody;
  let itemID: string;

  // const batchItemRequests: ItemIDToBatchItemRequestMap = event.Records.reduce(
  //   (acc, record) => {
  //     parsedMessageBody = JSON.parse(record.body);
  //     itemID =
  //     const requestForItem = acc[]
  //   },
  //   {},
  // );

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
