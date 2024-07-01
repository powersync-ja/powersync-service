export type JSONSchema = {
  definitions?: Record<string, any>;
  [key: string]: any;
};

export type IValidationRight = {
  valid: true;
};

export type ValidationLeft<T = string[]> = {
  valid: false;
  errors: T;
};

export type ValidationResponse<T, E = string[]> = ValidationLeft<E> | IValidationRight;

export type MicroValidator<T = any, E = string[]> = {
  validate: (data: T) => ValidationResponse<T, E>;
  toJSONSchema?: () => JSONSchema;
};
