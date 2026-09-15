/**
 * Authorization for AppSync Events. The same headers are sent when the
 * WebSocket is opened (inside the `header-…` subprotocol) and again with every
 * subscribe/publish message, so tokens are fetched fresh each time and a
 * refreshed token is picked up automatically on the next reconnect.
 */
export interface AppSyncAuth {
  /** Return the authorization headers for one operation. May be async. */
  headers(): Promise<Record<string, string>> | Record<string, string>;
}

type TokenSource = () => Promise<string> | string;

async function bearer(getToken: TokenSource): Promise<Record<string, string>> {
  const token = await getToken();
  if (typeof token !== 'string' || token.length === 0) {
    throw new Error('AppSync auth: token provider returned an empty token');
  }
  return { Authorization: token };
}

/** Amazon Cognito user pools: pass a function returning the current ID token. */
export function cognitoUserPool(getIdToken: TokenSource): AppSyncAuth {
  return { headers: () => bearer(getIdToken) };
}

/** OpenID Connect provider: pass a function returning the current JWT. */
export function oidc(getToken: TokenSource): AppSyncAuth {
  return { headers: () => bearer(getToken) };
}

/** AWS Lambda authorizer: pass a function returning the custom token your authorizer expects. */
export function lambdaAuthorizer(getToken: TokenSource): AppSyncAuth {
  return { headers: () => bearer(getToken) };
}

/** API key. Suitable for public, read-only channels only; the key ships to every client. */
export function apiKey(key: string): AppSyncAuth {
  return { headers: () => ({ 'x-api-key': key }) };
}

/**
 * Bring your own headers, e.g. AWS IAM SigV4 for a Node process. The signer
 * must return the signed headers for a POST to `https://{httpDomain}/event`
 * (see the AppSync Events docs, "IAM subprotocol format").
 */
export function customHeaders(provider: () => Promise<Record<string, string>> | Record<string, string>): AppSyncAuth {
  return { headers: provider };
}
