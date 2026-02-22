import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { DynamoDBClientFactory } from "./dynamodb-client";

interface MetricItem {
  pk: string;
  sk: string;
  count?: number;
}

export class MetricsRepository {
  private static readonly tableName = process.env.DYNAMODB_TABLE_NAME;
  private readonly client = DynamoDBDocumentClient.from(
    DynamoDBClientFactory.create({}),
  );

  public async getMetricCountFromDates(
    pk: string,
    fromDate: string,
    toDate: string,
  ): Promise<number> {
    const results = await this.client.send(
      new QueryCommand({
        TableName: MetricsRepository.tableName,
        KeyConditionExpression: "pk = :pk and sk between :fromDate and :toDate",
        ExpressionAttributeValues: {
          ":pk": pk,
          ":fromDate": `H#${fromDate}`,
          ":toDate": `H#${toDate}`,
        },
      }),
    );

    return ((results.Items ?? []) as MetricItem[]).reduce(
      (sum, item) => sum + (item.count ?? 0),
      0,
    );
  }
}
