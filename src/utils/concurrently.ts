import pLimit from "p-limit";

export const concurrently = async <T>(
  tasks: (() => Promise<T>)[],
  concurrency: number,
): Promise<T[]> => {
  const limit = pLimit(concurrency);

  return Promise.all(tasks.map((task) => limit(task)));
};
