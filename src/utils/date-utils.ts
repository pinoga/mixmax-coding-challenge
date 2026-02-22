import { addDays } from "date-fns/addDays";

export interface DateRange {
  type: "daily" | "hourly";
  fromDate: string;
  toDate: string;
}

export class DateUtils {
  public static nextDay(day: string): string {
    return this.getDay(addDays(new Date(day + "T00:00:00"), 1).toISOString());
  }

  public static prevDay(day: string): string {
    return this.getDay(addDays(new Date(day + "T00:00:00"), -1).toISOString());
  }

  public static getDay(date: string): string {
    return date.substring(0, 10);
  }

  public static getHour(date: string): string {
    return date.substring(11, 13);
  }

  public static lastHour(date: string): string {
    return this.getDay(date).concat("T23");
  }

  public static firstHour(date: string): string {
    return this.getDay(date).concat("T00");
  }
}
