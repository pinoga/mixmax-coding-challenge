import { DynamoDBMapper } from "../../src/mappers/dynamodb-mapper";

describe("DynamoDBMapper.decomposeDateRange", () => {
  it("should return a single daily range for a full same-day range", () => {
    const result = DynamoDBMapper.decomposeDateRange(
      "2024-01-15T00",
      "2024-01-15T23",
    );

    expect(result).toEqual([{ fromSK: "D#2024-01-15", toSK: "D#2024-01-15" }]);
  });

  it("should return a single hourly range for a partial same-day range", () => {
    const result = DynamoDBMapper.decomposeDateRange(
      "2024-01-15T05",
      "2024-01-15T18",
    );

    expect(result).toEqual([
      { fromSK: "H#2024-01-15T05", toSK: "H#2024-01-15T18" },
    ]);
  });

  it("should return a single hourly range for same-day partial start", () => {
    const result = DynamoDBMapper.decomposeDateRange(
      "2024-01-15T03",
      "2024-01-15T23",
    );

    expect(result).toEqual([
      { fromSK: "H#2024-01-15T03", toSK: "H#2024-01-15T23" },
    ]);
  });

  it("should return a single hourly range for same-day partial end", () => {
    const result = DynamoDBMapper.decomposeDateRange(
      "2024-01-15T00",
      "2024-01-15T18",
    );

    expect(result).toEqual([
      { fromSK: "H#2024-01-15T00", toSK: "H#2024-01-15T18" },
    ]);
  });

  it("should return only daily range for full multi-day range", () => {
    const result = DynamoDBMapper.decomposeDateRange(
      "2024-01-01T00",
      "2024-01-31T23",
    );

    expect(result).toEqual([{ fromSK: "D#2024-01-01", toSK: "D#2024-01-31" }]);
  });

  it("should return 3 segments for multi-day with partial edges", () => {
    const result = DynamoDBMapper.decomposeDateRange(
      "2024-01-15T05",
      "2024-01-20T18",
    );

    expect(result).toEqual([
      { fromSK: "H#2024-01-15T05", toSK: "H#2024-01-15T23" },
      { fromSK: "D#2024-01-16", toSK: "D#2024-01-19" },
      { fromSK: "H#2024-01-20T00", toSK: "H#2024-01-20T18" },
    ]);
  });

  it("should return 2 segments for multi-day with partial start only", () => {
    const result = DynamoDBMapper.decomposeDateRange(
      "2024-01-15T05",
      "2024-01-20T23",
    );

    expect(result).toEqual([
      { fromSK: "H#2024-01-15T05", toSK: "H#2024-01-15T23" },
      { fromSK: "D#2024-01-16", toSK: "D#2024-01-20" },
    ]);
  });

  it("should return 2 segments for multi-day with partial end only", () => {
    const result = DynamoDBMapper.decomposeDateRange(
      "2024-01-15T00",
      "2024-01-20T18",
    );

    expect(result).toEqual([
      { fromSK: "D#2024-01-15", toSK: "D#2024-01-19" },
      { fromSK: "H#2024-01-20T00", toSK: "H#2024-01-20T18" },
    ]);
  });

  it("should handle adjacent days with partial edges and no full days between", () => {
    const result = DynamoDBMapper.decomposeDateRange(
      "2024-01-15T05",
      "2024-01-16T18",
    );

    expect(result).toEqual([
      { fromSK: "H#2024-01-15T05", toSK: "H#2024-01-15T23" },
      { fromSK: "H#2024-01-16T00", toSK: "H#2024-01-16T18" },
    ]);
  });

  it("should handle month boundary correctly", () => {
    const result = DynamoDBMapper.decomposeDateRange(
      "2024-01-31T00",
      "2024-02-02T23",
    );

    expect(result).toEqual([{ fromSK: "D#2024-01-31", toSK: "D#2024-02-02" }]);
  });

  it("should handle year boundary correctly", () => {
    const result = DynamoDBMapper.decomposeDateRange(
      "2024-12-30T05",
      "2025-01-02T23",
    );

    expect(result).toEqual([
      { fromSK: "H#2024-12-30T05", toSK: "H#2024-12-30T23" },
      { fromSK: "D#2024-12-31", toSK: "D#2025-01-02" },
    ]);
  });
});
