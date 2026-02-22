import { MetricQuerySchema } from "./dto/metric-query";
import {
  MetricUpdatesMessage,
  MetricUpdatesMessageSchema,
} from "./dto/metric-updates-event";
import { MetricsRepository } from "./dynamodb/dynamodb-repository";
import { Logger } from "./logger/logger";
import { DynamoDBMapper } from "./mappers/dynamodb.mapper";

export interface DynamoDBMetricsResponse {
  userId: string;
  workspaceId: string;
  fromDate: string;
  toDate: string;
  count: number;
  metricId: string;
}

export interface UpdateItemRequest {
  sk: string;
  pk: string;
  inc: number;
  messageIds: string[];
}

type ItemIDToUpdateItemRequestMap = Record<string, UpdateItemRequest>;

export class MetricsService {
  public constructor(
    private readonly metricsRepository: MetricsRepository,
    private readonly logger: Logger = Logger.instance(),
  ) {}

  public queryMetricCount(query: MetricQuerySchema): Promise<number> {
    return this.metricsRepository.getMetricCountFromDates(query);
  }

  public aggregateMetricUpdateRecords(
    records: {
      body: string;
      messageId: string;
    }[],
  ): ItemIDToUpdateItemRequestMap {
    return records.reduce<ItemIDToUpdateItemRequestMap>((acc, record) => {
      let message: MetricUpdatesMessageSchema;
      try {
        message = MetricUpdatesMessage.validate(record.body);
      } catch (error) {
        this.logger.error({
          message: "Invalid MetricUpdatesMessage, skipping",
          meta: { error },
        });
        return acc;
      }
      const items = DynamoDBMapper.eventMessageToItems(message);

      for (const item of items) {
        const updateItemRequest: UpdateItemRequest | undefined = acc[item.id];
        if (updateItemRequest) {
          updateItemRequest.inc += message.count;
          updateItemRequest.messageIds.push(record.messageId);
        } else {
          acc[item.id] = {
            messageIds: [record.messageId],
            sk: item.sk,
            pk: item.pk,
            inc: message.count,
          };
        }
      }

      return acc;
    }, {});
  }
}
