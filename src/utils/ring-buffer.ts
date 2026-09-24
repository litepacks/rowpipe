/**
 * Fixed-capacity circular buffer with O(1) push and bounded O(N) memory complexity.
 */
export class RingBuffer<T> {
  private buffer: Array<T | undefined>;
  private capacity: number;
  private head = 0;
  private length = 0;

  constructor(capacity: number) {
    this.capacity = Math.max(1, capacity);
    this.buffer = new Array(this.capacity);
  }

  get size(): number {
    return this.length;
  }

  push(item: T): void {
    const index = (this.head + this.length) % this.capacity;
    this.buffer[index] = item;
    if (this.length < this.capacity) {
      this.length++;
    } else {
      this.head = (this.head + 1) % this.capacity;
    }
  }

  toArray(): T[] {
    const result: T[] = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      const index = (this.head + i) % this.capacity;
      result[i] = this.buffer[index] as T;
    }
    return result;
  }

  clear(): void {
    this.head = 0;
    this.length = 0;
    this.buffer.fill(undefined);
  }
}
