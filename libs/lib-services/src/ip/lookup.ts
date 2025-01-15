import * as net from 'node:net';
import * as dns from 'node:dns';
import * as dnsp from 'node:dns/promises';
import ip from 'ipaddr.js';

/**
 * Generate a custom DNS lookup function, that rejects specific IP ranges.
 *
 * @param reject_ip_ranges IPv4 and/or IPv6 subnets to reject, or 'local' to reject any IP that isn't public unicast.
 * @returns a function to use as the `lookup` option in `net.connect`.
 */
export function makeLookupFunction(reject_ip_ranges: string[]): net.LookupFunction | undefined {
  if (reject_ip_ranges.length == 0) {
    return undefined;
  }

  return (hostname, options, callback) => {
    resolveIp(hostname, reject_ip_ranges)
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
 * Resolve IP, and check that it is in an allowed range.
 */
export async function resolveIp(hostname: string, reject_ranges: string[]): Promise<dns.LookupAddress> {
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

  const parsed = ip.parse(resolvedAddress.address);
  const rejectLocal = reject_ranges.includes('local');
  const rejectSubnets = reject_ranges.filter((range) => range != 'local');

  const reject = { blocked: (rejectSubnets ?? []).map((r) => ip.parseCIDR(r)) };

  if (ip.subnetMatch(parsed, reject) == 'blocked') {
    // Ranges explicitly blocked, e.g. private IPv6 ranges
    throw new Error(`IPs in this range are not supported: ${resolvedAddress.address}`);
  }

  if (!rejectLocal) {
    return resolvedAddress;
  }

  if (parsed.kind() == 'ipv4' && parsed.range() == 'unicast') {
    // IPv4 - All good
    return resolvedAddress;
  } else if (parsed.kind() == 'ipv6' && parsed.range() == 'unicast') {
    // IPv6 - All good
    return resolvedAddress;
  } else {
    // Do not connect to any reserved IPs, including loopback and private ranges
    throw new Error(`IPs in this range are not supported: ${resolvedAddress.address}`);
  }
}
