/**
 * Return the larger of two LSNs.
 */
export function maxLsn(a: string | null | undefined, b: string | null | undefined): string | null {
  if (a == null) return b ?? null;
  if (b == null) return a;
  return a > b ? a : b;
}
