import {
  QueryCommand,
  UpdateItemCommand,
  UpdateItemCommandOutput,
} from "@aws-sdk/client-dynamodb";
import { MetricQuerySchema } from "../dto/metric-query";
import { DynamoDBMapper } from "../mappers/dynamodb-mapper";
import { DateRange } from "../utils/date-utils";
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

  public async getMetricCount(
    query: MetricQuerySchema,
    ranges: DateRange[],
  ): Promise<number> {
    const pk = DynamoDBMapper.queryRequestToPK(query);

    const counts = await Promise.all(
      ranges.map((range) => {
        const prefix = range.type === "daily" ? "D#" : "H#";
        return this.queryCountBySKRange(
          pk,
          `${prefix}${range.fromDate}`,
          `${prefix}${range.toDate}`,
        );
      }),
    );

    return counts.reduce((sum, c) => sum + c, 0);
  }

  private async queryCountBySKRange(
    pk: string,
    fromSK: string,
    toSK: string,
  ): Promise<number> {
    let count = 0;
    let exclusiveStartKey: Record<string, { S: string }> | undefined;

    do {
      const results = await this.client.send(
        new QueryCommand({
          TableName: MetricsRepository.tableName,
          KeyConditionExpression: "pk = :pk and sk between :fromSK and :toSK",
          ExpressionAttributeValues: {
            ":pk": { S: pk },
            ":fromSK": { S: fromSK },
            ":toSK": { S: toSK },
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
