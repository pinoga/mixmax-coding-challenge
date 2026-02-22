import { DynamoDBClient, UpdateItemCommand } from "@aws-sdk/client-dynamodb";
import { SQSBatchItemFailure, SQSBatchResponse, SQSEvent } from "aws-lambda";

export const main = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  const client = new DynamoDBClient({});
  const tableName = `feature-usage-${process.env.ENV || "local"}`;
  const batchChunkSize = Number(process.env.BATCH_CHUNK_SIZE) || 100;

  const batchItemFailures = new Array<SQSBatchItemFailure>();

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

  return { batchItemFailures };
};

module.exports = { main };
