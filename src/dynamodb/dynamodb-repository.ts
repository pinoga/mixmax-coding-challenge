import {
  QueryCommand,
  UpdateItemCommand,
  UpdateItemCommandOutput,
} from "@aws-sdk/client-dynamodb";
import { MetricQuerySchema } from "../dto/metric-query";
import { DynamoDBMapper } from "../mappers/dynamodb.mapper";
import { DynamoDBClientFactory } from "./dynamodb-client";

export interface IncrementMetricItem {
  sk: string;
  pk: string;
  inc: number;
}

export class MetricsRepository {
  private static readonly tableName = process.env.DYNAMODB_TABLE_NAME;

  public constructor(
    private readonly client = DynamoDBClientFactory.create({}),
  ) {}

  public async getMetricCountFromDates(
    query: MetricQuerySchema,
  ): Promise<number> {
    let count = 0;
    let exclusiveStartKey: Record<string, { S: string }> | undefined;

    do {
      const results = await this.client.send(
        new QueryCommand({
          TableName: MetricsRepository.tableName,
          KeyConditionExpression:
            "pk = :pk and sk between :fromDate and :toDate",
          ExpressionAttributeValues: {
            ":pk": { S: DynamoDBMapper.queryRequestToPK(query) },
            ":fromDate": { S: `H#${query.fromDate}` },
            ":toDate": { S: `H#${query.toDate}` },
          },
          ExclusiveStartKey: exclusiveStartKey,
        }),
      );

      count += (results.Items ?? []).reduce(
        (sum, item) => sum + Number(item.count?.N ?? 0),
        0,
      );

      exclusiveStartKey = results.LastEvaluatedKey as typeof exclusiveStartKey;
    } while (exclusiveStartKey);

    return count;
  }

  public async incrementMetricCount({
    inc,
    pk,
    sk,
  }: IncrementMetricItem): Promise<UpdateItemCommandOutput> {
    return this.client.send(
      new UpdateItemCommand({
        TableName: MetricsRepository.tableName,
        Key: {
          pk: { S: pk },
          sk: { S: sk },
        },
        UpdateExpression: "ADD #count :inc",
        ExpressionAttributeNames: { "#count": "count" },
        ExpressionAttributeValues: { ":inc": { N: inc.toString() } },
      }),
    );
  }
}
