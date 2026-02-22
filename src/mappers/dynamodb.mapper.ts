import { EventMessageBody } from "../types/event";

interface DynamoDBItemIdentifier {
  pk: string;
  sk: string;
  id: string;
}

export class DynamoDBMapper {
  public static eventMessageToItems(
    message: EventMessageBody,
  ): DynamoDBItemIdentifier[] {
    const dailySK = this.eventToDailySK(message);
    const hourlySK = this.eventToHourlySK(message);

    if (message.userId) {
      const userPK = `USR#${message.userId}#MET#${message.metricId}`;

      return [
        { pk: userPK, sk: dailySK, id: this.PKandSKToID(userPK, dailySK) },
        { pk: userPK, sk: hourlySK, id: this.PKandSKToID(userPK, hourlySK) },
      ];
    }
    const workspacePK = `WSP#${message.workspaceId}#MET#${message.metricId}`;

    return [
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
    ];
  }

  public static eventToHourlySK(message: EventMessageBody): string {
    return `H#${message.date}`;
  }

  public static eventToDailySK(message: EventMessageBody): string {
    return `D#${message.date.substring(0, 10)}`;
  }

  public static PKandSKToID(pk: string, sk: string): string {
    return `${pk}#${sk}`;
  }
}
