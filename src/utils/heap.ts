/**
 * Generic Binary Heap implementation supporting MinHeap, MaxHeap, and Priority Queue.
 */
export type Comparator<T> = (a: T, b: T) => number;

export class Heap<T> {
  private data: T[] = [];
  private comparator: Comparator<T>;

  constructor(comparator: Comparator<T>) {
    this.comparator = comparator;
  }

  get size(): number {
    return this.data.length;
  }

  isEmpty(): boolean {
    return this.data.length === 0;
  }

  peek(): T | undefined {
    return this.data[0];
  }

  push(value: T): void {
    this.data.push(value);
    this.bubbleUp(this.data.length - 1);
  }

  pop(): T | undefined {
    if (this.data.length === 0) return undefined;
    const top = this.data[0];
    const bottom = this.data.pop()!;
    if (this.data.length > 0) {
      this.data[0] = bottom;
      this.bubbleDown(0);
    }
    return top;
  }

  /**
   * Replaces the root element with a new element and re-balances the heap.
   */
  replace(value: T): T | undefined {
    const top = this.data[0];
    this.data[0] = value;
    this.bubbleDown(0);
    return top;
  }

  /**
   * Drains all elements from the heap in sorted order.
   */
  drain(): T[] {
    const result: T[] = [];
    while (!this.isEmpty()) {
      result.push(this.pop()!);
    }
    return result;
  }

  private bubbleUp(index: number): void {
    while (index > 0) {
      const parentIdx = (index - 1) >>> 1;
      if (this.comparator(this.data[index]!, this.data[parentIdx]!) < 0) {
        const temp = this.data[index]!;
        this.data[index] = this.data[parentIdx]!;
        this.data[parentIdx] = temp;
        index = parentIdx;
      } else {
        break;
      }
    }
  }

  private bubbleDown(index: number): void {
    const length = this.data.length;
    while (true) {
      const leftIdx = (index << 1) + 1;
      const rightIdx = leftIdx + 1;
      let smallest = index;

      if (
        leftIdx < length &&
        this.comparator(this.data[leftIdx]!, this.data[smallest]!) < 0
      ) {
        smallest = leftIdx;
      }

      if (
        rightIdx < length &&
        this.comparator(this.data[rightIdx]!, this.data[smallest]!) < 0
      ) {
        smallest = rightIdx;
      }

      if (smallest !== index) {
        const temp = this.data[index]!;
        this.data[index] = this.data[smallest]!;
        this.data[smallest] = temp;
        index = smallest;
      } else {
        break;
      }
    }
  }
}
