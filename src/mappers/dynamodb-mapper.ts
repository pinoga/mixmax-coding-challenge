import { addDays } from "date-fns/addDays";
import { MetricQuerySchema } from "../dto/metric-query";
import { MetricUpdatesMessageSchema } from "../dto/metric-updates-message";

interface SKRange {
  fromSK: string;
  toSK: string;
}

interface DynamoDBItemIdentifier {
  pk: string;
  sk: string;
  id: string;
}

export class DynamoDBMapper {
  public static eventMessageToItems(
    message: MetricUpdatesMessageSchema,
  ): DynamoDBItemIdentifier[] {
    const dailySK = this.eventToDailySK(message);
    const hourlySK = this.eventToHourlySK(message);
    const items = [];

    if (message.userId) {
      const userPK = this.userPK(message.userId, message.metricId);

      items.push(
        { pk: userPK, sk: dailySK, id: this.toItemID(userPK, dailySK) },
        { pk: userPK, sk: hourlySK, id: this.toItemID(userPK, hourlySK) },
      );
    }
    const workspacePK = this.workspacePK(message.workspaceId, message.metricId);

    items.push(
      {
        pk: workspacePK,
        sk: dailySK,
        id: this.toItemID(workspacePK, dailySK),
      },
      {
        pk: workspacePK,
        sk: hourlySK,
        id: this.toItemID(workspacePK, hourlySK),
      },
    );

    return items;
  }

  public static userPK(userId: string, metricId: string): string {
    return `USR#${userId}#MET#${metricId}`;
  }

  public static workspacePK(workspaceId: string, metricId: string): string {
    return `WSP#${workspaceId}#MET#${metricId}`;
  }

  public static eventToHourlySK(message: MetricUpdatesMessageSchema): string {
    return `H#${message.date}`;
  }

  public static eventToDailySK(message: MetricUpdatesMessageSchema): string {
    return `D#${message.date.substring(0, 10)}`;
  }

  public static queryRequestToPK(query: MetricQuerySchema): string {
    return query.userId
      ? this.userPK(query.userId, query.metricId)
      : this.workspacePK(query.workspaceId, query.metricId);
  }

  public static decomposeDateRange(
    fromDate: string,
    toDate: string,
  ): SKRange[] {
    const fromDay = fromDate.substring(0, 10);
    const fromHour = fromDate.substring(11, 13);
    const toDay = toDate.substring(0, 10);
    const toHour = toDate.substring(11, 13);

    const ranges: SKRange[] = [];

    if (fromDay === toDay) {
      if (fromHour === "00" && toHour === "23") {
        ranges.push({ fromSK: `D#${fromDay}`, toSK: `D#${fromDay}` });
      } else {
        ranges.push({ fromSK: `H#${fromDate}`, toSK: `H#${toDate}` });
      }
      return ranges;
    }

    // Partial first day
    if (fromHour !== "00") {
      ranges.push({ fromSK: `H#${fromDate}`, toSK: `H#${fromDay}T23` });
    }

    // Full days in the middle (and possibly the edges if they're full)
    const dailyStart = fromHour === "00" ? fromDay : this.nextDay(fromDay);
    const dailyEnd = toHour === "23" ? toDay : this.prevDay(toDay);

    if (dailyStart <= dailyEnd) {
      ranges.push({ fromSK: `D#${dailyStart}`, toSK: `D#${dailyEnd}` });
    }

    // Partial last day
    if (toHour !== "23") {
      ranges.push({ fromSK: `H#${toDay}T00`, toSK: `H#${toDate}` });
    }

    return ranges;
  }

  private static nextDay(day: string): string {
    return addDays(new Date(day + "T00:00:00"), 1)
      .toISOString()
      .substring(0, 10);
  }

  private static prevDay(day: string): string {
    return addDays(new Date(day + "T00:00:00"), -1)
      .toISOString()
      .substring(0, 10);
  }

  private static toItemID(pk: string, sk: string): string {
    return `${pk}#${sk}`;
  }
}
