import { differenceInDays, parse } from "date-fns";
import z from "zod";
import { DateValidator } from "../validators/date-validator";

export type MetricQuerySchema = z.infer<typeof MetricQuery.querySchema>;

export class MetricQuery {
  private static maxDateRangeInDays =
    Number(process.env.QUERY_MAX_DATE_RANGE_IN_DAYS) || 1825;
  public static querySchema = z
    .object({
      workspaceId: z.string().min(1),
      fromDate: DateValidator["YYYY-MM-DDThhRegExpDate"](),
      toDate: DateValidator["YYYY-MM-DDThhRegExpDate"](),
      metricId: z.string().min(1),
      userId: z.string().min(1).optional(),
    })
    .superRefine((data, ctx) => {
      const fromDate = parse(
        data.fromDate,
        DateValidator.dateFormat,
        new Date(),
      );
      const toDate = parse(data.toDate, DateValidator.dateFormat, new Date());

      if (differenceInDays(toDate, fromDate) > this.maxDateRangeInDays) {
        ctx.addIssue({
          code: "custom",
          message: `"toDate" must be at most ${this.maxDateRangeInDays} days from "fromDate"`,
          path: ["toDate"],
        });
      }
    });

  public static validate(query: Record<string, unknown>): MetricQuerySchema {
    return this.querySchema.parse(query);
  }
}
