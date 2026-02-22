import z from "zod";

export type MetricUpdatesMessageSchema = z.infer<
  typeof MetricUpdatesMessage.messageBodySchema
>;

export class MetricUpdatesMessage {
  public static messageBodySchema = z.object({
    workspaceId: z.string().min(1),
    count: z.number().positive(),
    date: z.string().min(1),
    metricId: z.string().min(1),
    userId: z.string().min(1).optional(),
  });

  public static validate(rawMessage: string): MetricUpdatesMessageSchema {
    const parsed = JSON.parse(rawMessage);
    return this.messageBodySchema.parse(parsed);
  }
}
