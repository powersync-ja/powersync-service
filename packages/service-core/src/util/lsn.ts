/**
 * Return the larger of two LSNs.
 */
export function maxLsn(a: string | null | undefined, b: string | null | undefined): string | null {
  if (a == null) return b ?? null;
  if (b == null) return a;
  return a > b ? a : b;
}

/**
 * Return the smaller of two LSNs, ignoring nulls.
 */
export function minLsn(a: string | null | undefined, b: string | null | undefined): string | null {
  if (a == null) return b ?? null;
  if (b == null) return a;
  return a < b ? a : b;
}
