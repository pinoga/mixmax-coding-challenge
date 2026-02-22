import { SQSEvent } from "aws-lambda";
import { MetricQuerySchema } from "../../src/dto/metric-query";

export const validQueryRequest = (
  overrides: Partial<MetricQuerySchema> = {},
): MetricQuerySchema =>
  ({
    metricId: "emails-sent",
    workspaceId: "ws-1",
    fromDate: "2024-01-01T00",
    toDate: "2024-01-31T23",
    ...overrides,
  }) as MetricQuerySchema;

export const sqsEvent = (
  records: { body: string; messageId: string }[],
): SQSEvent =>
  ({
    Records: records.map((r) => ({
      body: r.body,
      messageId: r.messageId,
    })),
  }) as unknown as SQSEvent;

export const validMessageBody = (overrides: Record<string, unknown> = {}) =>
  JSON.stringify({
    workspaceId: "ws-1",
    metricId: "emails-sent",
    count: 1,
    date: "2024-01-15T14",
    ...overrides,
  });
