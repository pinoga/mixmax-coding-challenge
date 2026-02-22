import { MetricQuery, MetricQuerySchema } from "../dto/metric-query";
import { MetricsRepository } from "../dynamodb/dynamodb-repository";
import { MetricsService } from "../services/metric-service";

const metricsRepository = new MetricsRepository();
const metricsService = new MetricsService(metricsRepository);

export interface DynamoDBMetricsResponse {
  userId?: string;
  workspaceId: string;
  fromDate: string;
  toDate: string;
  count: number;
  metricId: string;
}

export const main = async (
  request: MetricQuerySchema,
): Promise<DynamoDBMetricsResponse> => {
  try {
    const query = MetricQuery.validate(request);

    const metricCount = await metricsService.queryMetricCount(query);

    return {
      ...(request.userId && { userId: request.userId }),
      workspaceId: request.workspaceId,
      fromDate: request.fromDate,
      toDate: request.toDate,
      count: metricCount,
      metricId: request.metricId,
    } satisfies DynamoDBMetricsResponse;
  } catch (error) {
    throw new Error(`Couldn't query for metrics`, {
      cause: error,
    });
  }
};
