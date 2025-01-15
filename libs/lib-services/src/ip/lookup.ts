import * as net from 'node:net';
import * as dns from 'node:dns';
import * as dnsp from 'node:dns/promises';
import ip from 'ipaddr.js';

export interface LookupOptions {
  reject_ip_ranges: string[];
  reject_ipv6?: boolean;
}

/**
 * Generate a custom DNS lookup function, that rejects specific IP ranges.
 *
 * If hostname is an IP, this synchronously validates it.
 *
 * @returns a function to use as the `lookup` option in `net.connect`.
 */
export function makeHostnameLookupFunction(
  hostname: string,
  lookupOptions: LookupOptions
): net.LookupFunction | undefined {
  validateIpHostname(hostname, lookupOptions);
  return makeLookupFunction(lookupOptions);
}

/**
 * Generate a custom DNS lookup function, that rejects specific IP ranges.
 *
 * Note: Lookup functions are not used for IPs configured directly.
 * For those, validate the IP directly using validateIpHostname().
 *
 * @param reject_ip_ranges IPv4 and/or IPv6 subnets to reject, or 'local' to reject any IP that isn't public unicast.
 * @returns a function to use as the `lookup` option in `net.connect`.
 */
export function makeLookupFunction(lookupOptions: LookupOptions): net.LookupFunction | undefined {
  return (hostname, options, callback) => {
    resolveIp(hostname, lookupOptions)
      .then((resolvedAddress) => {
        if (options.all) {
          callback(null, [resolvedAddress]);
        } else {
          callback(null, resolvedAddress.address, resolvedAddress.family);
        }
      })
      .catch((err) => {
        callback(err, undefined as any, undefined);
      });
  };
}

/**
 * Validate IPs synchronously.
 *
 * If the hostname is not an ip, this does nothing.
 *
 * @param hostname IP or DNS name
 * @param options
 */
export function validateIpHostname(hostname: string, options: LookupOptions): void {
  const { reject_ip_ranges: reject_ranges } = options;
  if (!ip.isValid(hostname)) {
    // Treat as a DNS name.
    return;
  }

  const parsed = ip.parse(hostname);
  const rejectLocal = reject_ranges.includes('local');
  const rejectSubnets = reject_ranges.filter((range) => range != 'local');

  const reject = { blocked: (rejectSubnets ?? []).map((r) => ip.parseCIDR(r)) };

  if (options.reject_ipv6 && parsed.kind() == 'ipv6') {
    throw new Error('IPv6 not supported');
  }

  if (ip.subnetMatch(parsed, reject) == 'blocked') {
    // Ranges explicitly blocked, e.g. private IPv6 ranges
    throw new Error(`IPs in this range are not supported: ${hostname}`);
  }

  if (!rejectLocal) {
    return;
  }

  if (parsed.kind() == 'ipv4' && parsed.range() == 'unicast') {
    // IPv4 - All good
    return;
  } else if (parsed.kind() == 'ipv6' && parsed.range() == 'unicast') {
    // IPv6 - All good
    return;
  } else {
    // Do not connect to any reserved IPs, including loopback and private ranges
    throw new Error(`IPs in this range are not supported: ${hostname}`);
  }
}

/**
 * Resolve IP, and check that it is in an allowed range.
 */
export async function resolveIp(hostname: string, options: LookupOptions): Promise<dns.LookupAddress> {
  let resolvedAddress: dns.LookupAddress;
  if (net.isIPv4(hostname)) {
    // Direct ipv4 - all good so far
    resolvedAddress = { address: hostname, family: 4 };
  } else if (net.isIPv6(hostname) || net.isIPv4(hostname)) {
    // Direct ipv6 - all good so far
    resolvedAddress = { address: hostname, family: 6 };
  } else {
    // DNS name - resolve it
    resolvedAddress = await dnsp.lookup(hostname);
  }
  validateIpHostname(resolvedAddress.address, options);
  return resolvedAddress;
}
