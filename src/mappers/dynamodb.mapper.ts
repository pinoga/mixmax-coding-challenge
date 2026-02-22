import { MetricQuerySchema } from "../dto/metric-query";
import { MetricUpdatesMessageSchema } from "../dto/metric-updates-event";

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
      const userPK = `USR#${message.userId}#MET#${message.metricId}`;

      items.push(
        { pk: userPK, sk: dailySK, id: this.PKandSKToID(userPK, dailySK) },
        { pk: userPK, sk: hourlySK, id: this.PKandSKToID(userPK, hourlySK) },
      );
    }
    const workspacePK = `WSP#${message.workspaceId}#MET#${message.metricId}`;

    items.push(
      {
        pk: workspacePK,
        sk: dailySK,
        id: this.PKandSKToID(workspacePK, dailySK),
      },
      {
        pk: workspacePK,
        sk: hourlySK,
        id: this.PKandSKToID(workspacePK, hourlySK),
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

  private static PKandSKToID(pk: string, sk: string): string {
    return `${pk}#${sk}`;
  }
}
