export function escapeRegExp(string: string) {
  // https://stackoverflow.com/a/3561711/214837
  return string.replace(/[/\-\\^$*+?.()|[\]{}]/g, '\\$&');
}
