module.exports = {
  preset: "@shelf/jest-dynamodb",
  transform: {
    "^.+\\.tsx?$": "ts-jest",
    "^.+\\.m?js$": "ts-jest",
  },
  testMatch: ["<rootDir>/tests/e2e/**/*.spec.ts"],
  transformIgnorePatterns: ["node_modules/(?!(p-limit|yocto-queue)/)"],
  testTimeout: 30000,
};
