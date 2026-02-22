import { QueryCommand } from "@aws-sdk/lib-dynamodb";
import { MetricQuerySchema } from "../dto/metric-query";
import { DynamoDBMapper } from "../mappers/dynamodb.mapper";
import { DynamoDBClientFactory } from "./dynamodb-client";

interface MetricItem {
  pk: string;
  sk: string;
  count?: number;
}

export class MetricsRepository {
  private static readonly tableName = process.env.DYNAMODB_TABLE_NAME;

  public constructor(
    private readonly client = DynamoDBClientFactory.create({}),
  ) {}

  public async getMetricCountFromDates(
    query: MetricQuerySchema,
  ): Promise<number> {
    const results = await this.client.send(
      new QueryCommand({
        TableName: MetricsRepository.tableName,
        KeyConditionExpression: "pk = :pk and sk between :fromDate and :toDate",
        ExpressionAttributeValues: {
          ":pk": DynamoDBMapper.queryRequestToPK(query),
          ":fromDate": `H#${query.fromDate}`,
          ":toDate": `H#${query.toDate}`,
        },
      }),
    );

    return ((results.Items ?? []) as MetricItem[]).reduce(
      (sum, item) => sum + (item.count ?? 0),
      0,
    );
  }
}
