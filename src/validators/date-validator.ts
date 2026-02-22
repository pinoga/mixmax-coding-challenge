import { isValid } from "date-fns";
import zod, { ZodString } from "zod";

export class DateValidator {
  public static "YYYY-MM-DDThhRegExp" = new RegExp(
    "^\\d{4}-\\d{2}-\\d{2}T\\d{2}$",
  );

  public static "YYYY-MM-DDThhRegExpDate"(z: typeof zod): ZodString {
    return z
      .string()
      .refine((date) => isValid(date))
      .regex(this["YYYY-MM-DDThhRegExp"]);
  }
}
