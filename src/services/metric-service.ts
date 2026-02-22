import { MetricQuerySchema } from "../dto/metric-query";
import {
  MetricUpdatesMessage,
  MetricUpdatesMessageSchema,
} from "../dto/metric-updates-event";
import { MetricsRepository } from "../dynamodb/dynamodb-repository";
import { Logger } from "../logger/logger";
import { DynamoDBMapper } from "../mappers/dynamodb.mapper";
import { concurrently } from "../utils/concurrently";

export interface UpdateItemRequest {
  sk: string;
  pk: string;
  inc: number;
  messageIds: string[];
}
type ItemToRequestMap = Record<string, UpdateItemRequest>;
interface MetricUpdateAggregation {
  itemToRequestMap: ItemToRequestMap;
  messageToItemsMap: Record<string, Set<string>>;
}

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

  public async batchIncrementMetrics({
    messageToItemsMap,
    itemToRequestMap,
  }: MetricUpdateAggregation): Promise<string[]> {
    const messagesWithFailedItems = new Set<string>();
    const failedItems = new Set<string>();

    // Why not a plain Promise.all?
    // For smaller batches, it may not make much difference, but the Node.js can only
    // process so much I/O in parallel, so we may want to increase the SQS batch size independently
    // (to gain performance on deduplication) from the concurrency limit
    await concurrently(
      Object.entries(itemToRequestMap).map(
        ([itemID, request]) =>
          () =>
            this.metricsRepository
              .incrementMetricCount(request)
              .catch((error) => {
                failedItems.add(itemID);

                request.messageIds.forEach((message) =>
                  messagesWithFailedItems.add(message),
                );

                this.logger.error({
                  message:
                    "Error updating metrics for DynamoDB item. Requeueing",
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

    // Since we try to update every aggregated item in parallel, a single failure potentially affects multiple messages
    // To determine whether or not a message should be retried, we check if all items associated with a message actually failed
    // This favors under-counting in constrast to over-counting, as it only takes one successful item to consider a message successful
    return [
      ...messagesWithFailedItems.values().filter((messageWithFailedItem) => {
        return messageToItemsMap[messageWithFailedItem]!.values().every(
          (item) => failedItems.has(item),
        );
      }),
    ];
  }

  public aggregateMetricUpdateRecords(
    records: {
      body: string;
      messageId: string;
    }[],
  ): MetricUpdateAggregation {
    const messageToItemsMap: MetricUpdateAggregation["messageToItemsMap"] = {};
    const itemToRequestMap = records.reduce<ItemToRequestMap>((acc, record) => {
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

      const associatedItems = new Set<string>();
      messageToItemsMap[record.messageId] = associatedItems;

      for (const item of items) {
        associatedItems.add(item.id);
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

    return {
      itemToRequestMap,
      messageToItemsMap,
    };
  }
}
