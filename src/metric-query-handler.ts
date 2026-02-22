import { MetricQuery, MetricQuerySchema } from "./dto/metric-query";
import { MetricsRepository } from "./dynamodb/dynamodb-repository";
import { MetricsService } from "./metric-service";

const metricsRepository = new MetricsRepository();
const metricsService = new MetricsService(metricsRepository);

export const main = async (request: MetricQuerySchema) => {
  try {
    const query = MetricQuery.validate(request);

    const metricCount = await metricsService.queryMetricCount(query);

    return {
      userId: request.userId,
      workspaceId: request.workspaceId,
      fromDate: request.fromDate,
      toDate: request.toDate,
      count: metricCount,
      metricId: request.metricId,
    };
  } catch (error) {
    throw new Error(`Couldn't query for metrics`, {
      cause: error,
    });
  }
};

module.exports = { main };
