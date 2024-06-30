/**
 * Wait for a specified amount of time
 * @param {number} ms - The number of milliseconds to wait
 * @returns {Promise<void>}
 */
export function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
