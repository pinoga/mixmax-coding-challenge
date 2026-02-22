module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  transformIgnorePatterns: ["node_modules/(?!(p-limit|yocto-queue)/)"],
  transform: {
    "^.+\\.tsx?$": "ts-jest",
    "^.+\\.m?js$": "ts-jest",
  },
};
