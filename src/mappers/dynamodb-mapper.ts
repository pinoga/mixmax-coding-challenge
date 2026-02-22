import { MetricQuerySchema } from "../dto/metric-query";
import { MetricUpdatesMessageSchema } from "../dto/metric-updates-message";

interface DynamoDBItemIdentifier {
  pk: string;
  sk: string;
  id: string;
}

export class DynamoDBMapper {
  public static eventMessageToItems(
    message: MetricUpdatesMessageSchema,
  ): DynamoDBItemIdentifier[] {
    const dailySK = this.messageToDailySK(message);
    const hourlySK = this.messageToHourlySK(message);
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

  public static messageToHourlySK(message: MetricUpdatesMessageSchema): string {
    return `H#${message.date}`;
  }

  public static messageToDailySK(message: MetricUpdatesMessageSchema): string {
    return `D#${message.date.substring(0, 10)}`;
  }

  public static queryRequestToPK(query: MetricQuerySchema): string {
    return query.userId
      ? this.userPK(query.userId, query.metricId)
      : this.workspacePK(query.workspaceId, query.metricId);
  }

  private static toItemID(pk: string, sk: string): string {
    return `${pk}#${sk}`;
  }
}
