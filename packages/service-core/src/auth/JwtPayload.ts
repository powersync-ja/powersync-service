/**
 * Payload from a JWT, always signed.
 *
 * May have arbitrary additional parameters.
 */
export interface JwtPayload {
  /**
   * token_parameters.user_id
   */
  sub: string;

  iss?: string | undefined;
  exp: number;
  iat: number;
}
