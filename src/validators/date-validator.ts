import { isValid, parse } from "date-fns";
import z from "zod";

export class DateValidator {
  public static "YYYY-MM-DDThhRegExp" = new RegExp(
    "^\\d{4}-\\d{2}-\\d{2}T\\d{2}$",
  );

  public static dateFormat = "yyyy-MM-dd'T'HH";

  public static "YYYY-MM-DDThhRegExpDate"() {
    return z
      .string()
      .regex(this["YYYY-MM-DDThhRegExp"])
      .refine((date) => isValid(parse(date, this.dateFormat, new Date())));
  }
}
