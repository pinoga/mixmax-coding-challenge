import { MetricQuerySchema } from "./dto/metric-query";
import {
  MetricUpdatesMessage,
  MetricUpdatesMessageSchema,
} from "./dto/metric-updates-event";
import { MetricsRepository } from "./dynamodb/dynamodb-repository";
import { Logger } from "./logger/logger";
import { DynamoDBMapper } from "./mappers/dynamodb.mapper";
import { concurrently } from "./utils/concurrently";

export interface UpdateItemRequest {
  sk: string;
  pk: string;
  inc: number;
  messageIds: string[];
}

type ItemIDToUpdateItemRequestMap = Record<string, UpdateItemRequest>;

export class MetricsService {
  public static readonly batchUpdateConcurrency =
    Number(process.env.DYNAMODB_WRITE_CONCURRENCY) || 300;

  public constructor(
    private readonly metricsRepository: MetricsRepository = new MetricsRepository(),
    private readonly logger: Logger = Logger.instance(),
  ) {}

  public queryMetricCount(query: MetricQuerySchema): Promise<number> {
    return this.metricsRepository.getMetricCountFromDates(query);
  }

  public async batchIncrementMetrics(
    batchItemRequests: ItemIDToUpdateItemRequestMap,
  ): Promise<Set<string>> {
    const batchItemFailedMessageIDs = new Set<string>();

    // Why not a plain Promise.all?
    // For smaller batches, it may not make much difference, but the Node.js can only
    // process so much I/O in parallel, so we may want to increase the SQS batch size independently
    // (to gain performance on deduplication) from the concurrency limit
    await concurrently(
      Object.values(batchItemRequests).map(
        (request) => () =>
          this.metricsRepository
            .incrementMetricCount(request)
            .catch((error) => {
              request.messageIds.forEach((messageId) => {
                batchItemFailedMessageIDs.add(messageId);
              });
              this.logger.error({
                message: "Error updating metrics for DynamoDB item. Requeueing",
                meta: {
                  messageIds: request.messageIds,
                  error,
                  pk: request.pk,
                  sk: request.sk,
                  inc: request.inc,
                },
              });
            }),
      ),
      MetricsService.batchUpdateConcurrency,
    );

    return batchItemFailedMessageIDs;
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
