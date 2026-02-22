export interface EventMessageBody {
  workspaceId: string;
  userId?: string;
  metricId: string;
  count: number;
  date: string;
}
