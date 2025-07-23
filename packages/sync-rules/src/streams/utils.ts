export function* cartesianProduct<T>(...sets: T[][]): Generator<T[]> {
  if (sets.length == 0) {
    yield [];
    return;
  }

  const [head, ...tail] = sets;
  for (let h of head) {
    const remainder = cartesianProduct(...tail);
    for (let r of remainder) yield [h, ...r];
  }
}
