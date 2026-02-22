import z from "zod";
import { DateValidator } from "../validators/date-validator";

export type MetricUpdatesMessageSchema = z.infer<
  typeof MetricUpdatesMessage.messageBodySchema
>;

export class MetricUpdatesMessage {
  public static messageBodySchema = z.object({
    workspaceId: z.string().min(1),
    count: z.number().positive(),
    date: z.string().min(1).regex(DateValidator["YYYY-MM-DDThhRegExp"]),
    metricId: z.string().min(1),
    userId: z.string().min(1).optional(),
  });

  public static validate(rawMessage: string): MetricUpdatesMessageSchema {
    const parsed = JSON.parse(rawMessage);
    return this.messageBodySchema.parse(parsed);
  }
}
