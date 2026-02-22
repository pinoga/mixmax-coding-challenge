import { SQSEvent } from "aws-lambda";
import { MetricQuerySchema } from "../../src/dto/metric-query";

const mockSend = jest.fn();
jest.mock("../../src/dynamodb/dynamodb-client", () => ({
  DynamoDBClientFactory: {
    create: jest.fn(() => ({
      send: mockSend,
    })),
  },
}));

import { main as queryHandler } from "../../src/metric-query-handler";
import { main as consumer } from "../../src/metric-updates-consumer";

describe("metric-query-handler", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should return usage data for a user query", async () => {
    mockSend.mockResolvedValue({
      Items: [{ count: 3 }, { count: 2 }],
      Count: 2,
    });

    const result = await queryHandler({
      metricId: "emails-sent",
      workspaceId: "ws-1",
      userId: "user-1",
      fromDate: "2024-01-01T00",
      toDate: "2024-01-31T23",
    } as MetricQuerySchema);

    expect(result.count).toBe(5);
    expect(result.metricId).toBe("emails-sent");
  });
});

describe("metric-updates-consumer", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should process a valid SQS event", async () => {
    mockSend.mockResolvedValue({});

    await consumer({
      Records: [
        {
          body: JSON.stringify({
            workspaceId: "ws-1",
            metricId: "emails-sent",
            count: 1,
            date: "2024-01-15T14",
          }),
          messageId: "msg-1",
        },
      ],
    } as unknown as SQSEvent);

    // 2 writes per workspace entity: hourly (H#) and daily (D#)
    expect(mockSend).toHaveBeenCalledTimes(2);
  });
});
