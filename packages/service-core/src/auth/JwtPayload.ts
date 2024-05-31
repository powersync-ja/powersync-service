export interface JwtPayload {
  /**
   * user_id
   */
  sub: string;
  parameters: Record<string, any>;

  iss?: string | undefined;
  exp: number;
  iat: number;
}
