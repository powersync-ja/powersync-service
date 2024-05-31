/**
 * Variation on a leaky bucket rate limiter.
 *
 * The base is a leaky bucket:
 *  * The bucket is filled at a certain rate
 *  * The bucket has a max capacity
 *
 * This gives an initial burst capacity, then continues
 * at the refill rate when the burst capacity is depleted.
 *
 * This variation introduces an additional quadratic delay
 * during the initial burst, which prevents the burst capacity
 * from being consumed immediately. The steady-state rate
 * is still the same.
 *
 *
 * Steady state rate: x requests / ms.
 * Steady state period: 1 / rate
 *
 * capacity: number of requests before steady state
 *
 * capacity == max_capacity: no delay between requests
 * capacity = 0: delay = Steady state period
 *
 * variable_delay = (max_capacity - capacity)^2 / max_capacity^2 * period
 */
export class LeakyBucket {
  private capacity: number;
  private lastRequest: number;
  private maxCapacity: number;
  private periodMs: number;

  public lastGrantedRequest: number;

  constructor(options: { maxCapacity: number; periodMs: number }) {
    this.capacity = options.maxCapacity;
    this.maxCapacity = options.maxCapacity;
    this.periodMs = options.periodMs;
    this.lastRequest = 0;
    this.lastGrantedRequest = 0;
  }

  allowed(): boolean {
    const now = Date.now();
    const elapsed = now - this.lastRequest;
    const leaked = elapsed / this.periodMs;
    this.capacity = Math.min(this.capacity + leaked, this.maxCapacity);
    this.lastRequest = now;

    const capacityUsed = this.maxCapacity - this.capacity;
    const variableDelay = ((capacityUsed * capacityUsed) / (this.maxCapacity * this.maxCapacity)) * this.periodMs;

    if (this.capacity >= 1 && variableDelay <= now - this.lastGrantedRequest) {
      this.capacity -= 1;
      this.lastGrantedRequest = now;
      return true;
    } else {
      return false;
    }
  }

  reset() {
    this.capacity = this.maxCapacity;
    this.lastGrantedRequest = 0;
  }
}
