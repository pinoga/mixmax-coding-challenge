import { UpdateItemCommandOutput } from "@aws-sdk/client-dynamodb";
import { QueryCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { MetricQuerySchema } from "../dto/metric-query";
import { DynamoDBMapper } from "../mappers/dynamodb.mapper";
import { DynamoDBClientFactory } from "./dynamodb-client";

interface MetricItem {
  pk: string;
  sk: string;
  count?: number;
}

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
    let exclusiveStartKey: Record<string, unknown> | undefined;

    do {
      const results = await this.client.send(
        new QueryCommand({
          TableName: MetricsRepository.tableName,
          KeyConditionExpression:
            "pk = :pk and sk between :fromDate and :toDate",
          ExpressionAttributeValues: {
            ":pk": DynamoDBMapper.queryRequestToPK(query),
            ":fromDate": `H#${query.fromDate}`,
            ":toDate": `H#${query.toDate}`,
          },
          ExclusiveStartKey: exclusiveStartKey,
        }),
      );

      count += ((results.Items ?? []) as MetricItem[]).reduce(
        (sum, item) => sum + (item.count ?? 0),
        0,
      );

      exclusiveStartKey = results.LastEvaluatedKey;
    } while (exclusiveStartKey);

    return count;
  }

  public async incrementMetricCount({
    inc,
    pk,
    sk,
  }: IncrementMetricItem): Promise<UpdateItemCommandOutput> {
    return this.client.send(
      new UpdateCommand({
        TableName: MetricsRepository.tableName,
        Key: {
          pk,
          sk,
        },
        UpdateExpression: "ADD #count :inc",
        ExpressionAttributeNames: { "#count": "count" },
        ExpressionAttributeValues: { ":inc": inc },
      }),
    );
  }
}
