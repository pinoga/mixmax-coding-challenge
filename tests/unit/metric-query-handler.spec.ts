import { MetricQuerySchema } from "../../src/dto/metric-query";
import { validQueryRequest } from "./helpers";

const mockSend = jest.fn();
jest.mock("@aws-sdk/client-dynamodb", () => {
  const actual = jest.requireActual("@aws-sdk/client-dynamodb");
  return {
    ...actual,
    DynamoDBClient: jest.fn().mockImplementation(() => ({
      send: mockSend,
    })),
  };
});

import { main as queryHandler } from "../../src/handlers/metric-query-handler";

describe("metric-query-handler", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should return usage data for a workspace query", async () => {
    mockSend.mockResolvedValue({
      Items: [{ count: { N: "3" } }, { count: { N: "2" } }],
    });

    const result = await queryHandler(validQueryRequest());

    expect(result.count).toBe(5);
    expect(result.metricId).toBe("emails-sent");
    expect(result.workspaceId).toBe("ws-1");
    expect(result).not.toHaveProperty("userId");
  });

  it("should return usage data for a user query", async () => {
    mockSend.mockResolvedValue({
      Items: [{ count: { N: "7" } }],
    });

    const result = await queryHandler(validQueryRequest({ userId: "user-1" }));

    expect(result.count).toBe(7);
    expect(result.userId).toBe("user-1");
  });

  it("should return count 0 when no items found", async () => {
    mockSend.mockResolvedValue({ Items: [] });

    const result = await queryHandler(validQueryRequest());

    expect(result.count).toBe(0);
  });

  it("should return count 0 when Items is undefined", async () => {
    mockSend.mockResolvedValue({});

    const result = await queryHandler(validQueryRequest());

    expect(result.count).toBe(0);
  });

  it("should handle items with missing count field", async () => {
    mockSend.mockResolvedValue({
      Items: [
        { pk: { S: "WSP#ws-1#MET#emails-sent" }, sk: { S: "H#2024-01-15T14" } },
      ],
    });

    const result = await queryHandler(validQueryRequest());

    expect(result.count).toBe(0);
  });

  it("should paginate through multiple result pages", async () => {
    mockSend
      .mockResolvedValueOnce({
        Items: [{ count: { N: "10" } }],
        LastEvaluatedKey: {
          pk: { S: "WSP#ws-1" },
          sk: { S: "H#2024-01-15T12" },
        },
      })
      .mockResolvedValueOnce({
        Items: [{ count: { N: "5" } }],
        LastEvaluatedKey: {
          pk: { S: "WSP#ws-1" },
          sk: { S: "H#2024-01-20T00" },
        },
      })
      .mockResolvedValueOnce({
        Items: [{ count: { N: "3" } }],
      });

    const result = await queryHandler(validQueryRequest());

    expect(mockSend).toHaveBeenCalledTimes(3);
    expect(result.count).toBe(18);
  });

  it("should throw on missing metricId", async () => {
    await expect(
      queryHandler(validQueryRequest({ metricId: "" })),
    ).rejects.toThrow();
  });

  it("should throw on missing workspaceId", async () => {
    await expect(
      queryHandler(validQueryRequest({ workspaceId: "" })),
    ).rejects.toThrow();
  });

  it("should throw on invalid fromDate format", async () => {
    await expect(
      queryHandler(
        validQueryRequest({ fromDate: "2024-01-01" } as MetricQuerySchema),
      ),
    ).rejects.toThrow();
  });

  it("should throw on invalid toDate format", async () => {
    await expect(
      queryHandler(
        validQueryRequest({ toDate: "not-a-date" } as MetricQuerySchema),
      ),
    ).rejects.toThrow();
  });

  it("should throw on invalid date values (e.g. month 13)", async () => {
    await expect(
      queryHandler(
        validQueryRequest({ fromDate: "2024-13-01T00" } as MetricQuerySchema),
      ),
    ).rejects.toThrow();
  });

  it("should throw when date range exceeds max allowed days", async () => {
    await expect(
      queryHandler(
        validQueryRequest({
          fromDate: "2020-01-01T00",
          toDate: "2025-02-01T00",
        } as MetricQuerySchema),
      ),
    ).rejects.toThrow();
  });

  it("should throw when DynamoDB errors", async () => {
    mockSend.mockRejectedValue(new Error("DynamoDB unavailable"));

    await expect(queryHandler(validQueryRequest())).rejects.toThrow(
      "Couldn't query for metrics",
    );
  });
});
