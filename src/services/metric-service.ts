import { MetricQuerySchema } from "../dto/metric-query";
import {
  MetricUpdatesMessage,
  MetricUpdatesMessageSchema,
} from "../dto/metric-updates-message";
import { MetricsRepository } from "../dynamodb/dynamodb-repository";
import { Logger } from "../logger/logger";
import { DynamoDBMapper } from "../mappers/dynamodb-mapper";
import { concurrently } from "../utils/concurrently";
import { DateRange, DateUtils } from "../utils/date-utils";

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
    const ranges = MetricsService.decomposeDateRange(
      query.fromDate,
      query.toDate,
    );
    return this.metricsRepository.getMetricCount(query, ranges);
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
    // This favors under-counting in contrast to over-counting, as it only takes one successful item to consider a message successful
    return [
      ...messagesWithFailedItems.values().filter((messageWithFailedItem) => {
        return messageToItemsMap[messageWithFailedItem]!.values().every(
          (item) => failedItems.has(item),
        );
      }),
    ];
  }

  /**
   * Given an array of event records, aggregate the counts by item identifier
   */
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
      const items = DynamoDBMapper.toAssociatedItems(message);

      messageToItemsMap[record.messageId] = new Set(
        items.map((item) => item.id),
      );

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

    return {
      itemToRequestMap,
      messageToItemsMap,
    };
  }

  /**
   * Given an initial and final date strings (in the {@link DateValidator.dateFormat} format),
   * returns an array of ranges, each representing a query to the metric repository
   */
  public static decomposeDateRange(
    fromDate: string,
    toDate: string,
  ): DateRange[] {
    const fromDay = DateUtils.getDay(fromDate);
    const fromHour = DateUtils.getHour(fromDate);
    const toDay = DateUtils.getDay(toDate);
    const toHour = DateUtils.getHour(toDate);

    const partialStart = fromHour !== "00";
    const partialEnd = toHour !== "23";

    // same day range
    if (fromDay === toDay) {
      if (!partialStart && !partialEnd) {
        return [{ type: "daily", fromDate: fromDay, toDate: fromDay }];
      }
      return [{ type: "hourly", fromDate, toDate }];
    }

    // full days range
    if (!partialStart && !partialEnd) {
      return [{ type: "daily", fromDate: fromDay, toDate: toDay }];
    }

    // first day is full, last day is partial
    if (!partialStart) {
      return [
        { type: "daily", fromDate: fromDay, toDate: DateUtils.prevDay(toDay) },
        {
          type: "hourly",
          fromDate: DateUtils.firstHour(toDate),
          toDate: toDate,
        },
      ];
    }

    // first day is partial, last day is full
    if (!partialEnd) {
      return [
        { type: "hourly", fromDate, toDate: DateUtils.lastHour(fromDate) },
        { type: "daily", fromDate: DateUtils.nextDay(fromDay), toDate: toDay },
      ];
    }

    const firstDay: DateRange = {
      type: "hourly",
      fromDate,
      toDate: DateUtils.lastHour(fromDate),
    };
    const lastDay: DateRange = {
      type: "hourly",
      fromDate: DateUtils.firstHour(toDate),
      toDate: toDate,
    };

    // two partial days, but no days in between
    if (DateUtils.nextDay(fromDay) === toDay) {
      return [firstDay, lastDay];
    }

    const fullDays: DateRange = {
      type: "daily",
      fromDate: DateUtils.nextDay(fromDay),
      toDate: DateUtils.prevDay(toDay),
    };

    return [firstDay, fullDays, lastDay];
  }
}
