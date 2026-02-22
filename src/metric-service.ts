import { MetricQuerySchema } from "./dto/metric-query";
import { MetricsRepository } from "./dynamodb/dynamodb-repository";

export interface DynamoDBMetricsResponse {
  userId: string;
  workspaceId: string;
  fromDate: string;
  toDate: string;
  count: number;
  metricId: string;
}

export class MetricsService {
  public constructor(private readonly metricsRepository: MetricsRepository) {}

  public queryMetricCount(query: MetricQuerySchema): Promise<number> {
    return this.metricsRepository.getMetricCountFromDates(query);
  }
}
