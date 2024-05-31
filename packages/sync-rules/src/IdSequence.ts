export class IdSequence {
  private count = 0;

  nextId() {
    return `${++this.count}`;
  }
}
