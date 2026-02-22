import {
  DynamoDBClient as DynamoDBAWSClient,
  DynamoDBClientConfig,
} from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

// Why is this useful? We may want to add instrumentation and meaningful default to our clients
export class DynamoDBClientFactory {
  public static create(options: DynamoDBClientConfig) {
    return DynamoDBDocumentClient.from(
      new DynamoDBAWSClient({
        requestHandler: {
          httpsAgent: {
            maxSockets: Number(process.env.DYNAMODB_CLIENT_MAX_SOCKETS) || 50,
          },
          requestTimeout:
            Number(process.env.DYNAMODB_CLIENT_REQUEST_TIMEOUT_MS) || 3000,
        },
        maxAttempts: 3,
        ...options,
      }),
    );
  }
}
