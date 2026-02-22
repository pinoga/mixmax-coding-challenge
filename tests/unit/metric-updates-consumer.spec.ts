import { sqsEvent, validMessageBody } from "./helpers";

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

import { main as consumer } from "../../src/metric-updates-consumer";

describe("metric-updates-consumer", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should write hourly and daily for workspace-only message", async () => {
    mockSend.mockResolvedValue({});

    const result = await consumer(
      sqsEvent([{ body: validMessageBody(), messageId: "msg-1" }]),
    );

    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(result.batchItemFailures).toHaveLength(0);
  });

  it("should write 4 items when userId is provided (user hourly + daily, workspace hourly + daily)", async () => {
    mockSend.mockResolvedValue({});

    const result = await consumer(
      sqsEvent([
        {
          body: validMessageBody({ userId: "user-1" }),
          messageId: "msg-1",
        },
      ]),
    );

    expect(mockSend).toHaveBeenCalledTimes(4);
    expect(result.batchItemFailures).toHaveLength(0);
  });

  it("should aggregate counts for duplicate PK+SK within batch", async () => {
    mockSend.mockResolvedValue({});

    await consumer(
      sqsEvent([
        { body: validMessageBody({ count: 3 }), messageId: "msg-1" },
        { body: validMessageBody({ count: 7 }), messageId: "msg-2" },
      ]),
    );

    // Same workspace + metric + date = same PK+SK, so should aggregate into 2 writes (hourly + daily), not 4
    expect(mockSend).toHaveBeenCalledTimes(2);
  });

  it("should not aggregate messages with different metrics", async () => {
    mockSend.mockResolvedValue({});

    await consumer(
      sqsEvent([
        {
          body: validMessageBody({ metricId: "emails-sent" }),
          messageId: "msg-1",
        },
        {
          body: validMessageBody({ metricId: "logins" }),
          messageId: "msg-2",
        },
      ]),
    );

    // 2 different metrics × 2 writes each (hourly + daily) = 4
    expect(mockSend).toHaveBeenCalledTimes(4);
  });

  it("should not aggregate messages with different dates", async () => {
    mockSend.mockResolvedValue({});

    await consumer(
      sqsEvent([
        {
          body: validMessageBody({ date: "2024-01-15T14" }),
          messageId: "msg-1",
        },
        {
          body: validMessageBody({ date: "2024-01-15T15" }),
          messageId: "msg-2",
        },
      ]),
    );

    // Same daily SK but different hourly SK = 3 unique writes (1 shared daily + 2 hourly)
    expect(mockSend).toHaveBeenCalledTimes(3);
  });

  it("should skip invalid messages and process valid ones", async () => {
    mockSend.mockResolvedValue({});

    const result = await consumer(
      sqsEvent([
        { body: "not valid json", messageId: "msg-bad" },
        { body: validMessageBody(), messageId: "msg-good" },
      ]),
    );

    // Only the valid message produces writes
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(result.batchItemFailures).toHaveLength(0);
  });

  it("should skip messages with missing required fields", async () => {
    mockSend.mockResolvedValue({});

    const result = await consumer(
      sqsEvent([
        {
          body: JSON.stringify({ workspaceId: "ws-1" }),
          messageId: "msg-incomplete",
        },
        { body: validMessageBody(), messageId: "msg-good" },
      ]),
    );

    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(result.batchItemFailures).toHaveLength(0);
  });

  it("should skip messages with invalid date format", async () => {
    mockSend.mockResolvedValue({});

    const result = await consumer(
      sqsEvent([
        {
          body: validMessageBody({ date: "not-a-date" }),
          messageId: "msg-bad-date",
        },
        { body: validMessageBody(), messageId: "msg-good" },
      ]),
    );

    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(result.batchItemFailures).toHaveLength(0);
  });

  it("should skip messages with non-positive count", async () => {
    mockSend.mockResolvedValue({});

    const result = await consumer(
      sqsEvent([
        {
          body: validMessageBody({ count: 0 }),
          messageId: "msg-zero",
        },
        {
          body: validMessageBody({ count: -1 }),
          messageId: "msg-negative",
        },
        { body: validMessageBody(), messageId: "msg-good" },
      ]),
    );

    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(result.batchItemFailures).toHaveLength(0);
  });

  it("should report failed messageIds when DynamoDB write fails", async () => {
    mockSend.mockRejectedValue(new Error("DynamoDB throttle"));

    const result = await consumer(
      sqsEvent([{ body: validMessageBody(), messageId: "msg-1" }]),
    );

    expect(result.batchItemFailures).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ itemIdentifier: "msg-1" }),
      ]),
    );
  });

  it("should report all aggregated messageIds when a shared write fails", async () => {
    mockSend.mockRejectedValue(new Error("DynamoDB throttle"));

    const result = await consumer(
      sqsEvent([
        { body: validMessageBody({ count: 1 }), messageId: "msg-1" },
        { body: validMessageBody({ count: 2 }), messageId: "msg-2" },
      ]),
    );

    const failedIds = result.batchItemFailures.map((f) => f.itemIdentifier);
    expect(failedIds).toContain("msg-1");
    expect(failedIds).toContain("msg-2");
  });

  it("should only report failed messageIds, not successful ones (partial failure)", async () => {
    // metric-a writes succeed (hourly + daily), metric-b writes fail (hourly + daily)
    mockSend.mockImplementation((command: { input: { Key: { pk: { S: string } } } }) => {
      if (command.input.Key?.pk?.S?.includes("metric-b")) {
        return Promise.reject(new Error("throttle"));
      }
      return Promise.resolve({});
    });

    const result = await consumer(
      sqsEvent([
        {
          body: validMessageBody({ metricId: "metric-a" }),
          messageId: "msg-success",
        },
        {
          body: validMessageBody({ metricId: "metric-b" }),
          messageId: "msg-fail",
        },
      ]),
    );

    const failedIds = result.batchItemFailures.map((f) => f.itemIdentifier);
    expect(failedIds).toContain("msg-fail");
    expect(failedIds).not.toContain("msg-success");
  });

  it("should return empty failures when all writes succeed", async () => {
    mockSend.mockResolvedValue({});

    const result = await consumer(
      sqsEvent([
        { body: validMessageBody(), messageId: "msg-1" },
        {
          body: validMessageBody({ metricId: "logins" }),
          messageId: "msg-2",
        },
      ]),
    );

    expect(result.batchItemFailures).toHaveLength(0);
  });

  it("should handle empty Records array", async () => {
    const result = await consumer(sqsEvent([]));

    expect(mockSend).not.toHaveBeenCalled();
    expect(result.batchItemFailures).toHaveLength(0);
  });
});
