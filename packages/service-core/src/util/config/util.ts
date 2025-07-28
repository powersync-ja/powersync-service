import * as urijs from 'uri-js';

export function getSupabaseJwksUrl(connection: any) {
  if (connection == null) {
    return null;
  } else if (connection.type != 'postgresql') {
    return null;
  }

  let hostname: string | undefined = connection.hostname;
  if (hostname == null && typeof connection.uri == 'string') {
    hostname = urijs.parse(connection.uri).host;
  }
  if (hostname == null) {
    return null;
  }

  const match = /db.(\w+).supabase.co/.exec(hostname);
  if (match == null) {
    return null;
  }
  const projectId = match[1];

  return `https://${projectId}.supabase.co/auth/v1/.well-known/jwks.json`;
}
