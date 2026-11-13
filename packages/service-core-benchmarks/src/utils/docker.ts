import { request } from 'node:http';

export interface DockerContainer {
  Id: string;
  Ports: { IP?: string; PrivatePort: number; PublicPort?: number; Type: string }[];
}

export interface DockerResourceSample {
  cpuTotalNs: number;
  memoryBytes: number;
}

export interface DockerDatabaseSampler {
  listContainers(): Promise<DockerContainer[]>;
  sample(containerId: string): Promise<DockerResourceSample>;
}

const socketPath = process.env.DOCKER_HOST?.startsWith('unix://')
  ? process.env.DOCKER_HOST.slice('unix://'.length)
  : '/var/run/docker.sock';

export async function dockerGet(path: string): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const req = request({ socketPath, path, method: 'GET', timeout: 2_000 }, (response) => {
      let body = '';
      response.setEncoding('utf8');
      response.on('data', (chunk) => {
        body += chunk;
      });
      response.on('end', () => {
        if (response.statusCode !== 200) return reject(new Error(`Docker API returned ${response.statusCode}`));
        try {
          resolve(JSON.parse(body));
        } catch (error) {
          reject(error);
        }
      });
    });
    req.on('timeout', () => req.destroy(new Error('Docker API request timed out')));
    req.on('error', reject);
    req.end();
  });
}

export const defaultDockerSampler: DockerDatabaseSampler = {
  async listContainers() {
    return (await dockerGet('/containers/json')) as DockerContainer[];
  },
  async sample(containerId) {
    return parseDockerResourceSample(
      await dockerGet(`/containers/${encodeURIComponent(containerId)}/stats?stream=false&one-shot=true`)
    );
  }
};

export function parseDockerResourceSample(value: unknown): DockerResourceSample {
  const stats = value as {
    cpu_stats?: { cpu_usage?: { total_usage?: number } };
    memory_stats?: { usage?: number; stats?: { inactive_file?: number; total_inactive_file?: number } };
  };
  const cpuTotalNs = stats?.cpu_stats?.cpu_usage?.total_usage;
  const usage = stats?.memory_stats?.usage;
  const inactive = stats?.memory_stats?.stats?.inactive_file ?? stats?.memory_stats?.stats?.total_inactive_file ?? 0;
  if (!Number.isFinite(cpuTotalNs) || !Number.isFinite(usage) || !Number.isFinite(inactive) || usage! < inactive) {
    throw new Error('Docker API returned invalid CPU or memory statistics');
  }
  return { cpuTotalNs: cpuTotalNs!, memoryBytes: usage! - inactive };
}
