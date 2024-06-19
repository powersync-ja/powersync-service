import * as dotenv from 'dotenv';
import * as t from 'zod';

const string = t.string();
const number = t
  .string()
  .refine((value) => !isNaN(parseInt(value)))
  .transform((value) => parseInt(value));

const convertToBoolean = (value: string) => {
  switch (value) {
    case '1':
    case 'true': {
      return true;
    }
    case '0':
    case 'false': {
      return false;
    }
  }
};
const boolean = t
  .string()
  .refine((value) => ['0', '1', 'true', 'false'].includes(value))
  .transform(convertToBoolean);

const list = t.string().transform((value) => value.split(','));

export const DefaultSchema = t.object({});
export type DefaultSchema = t.infer<typeof DefaultSchema>;

export const collectEnvironmentVariablesFromSchema = <T extends t.ZodType>(
  schema: T,
  override?: Record<string, any>
): t.infer<T> => {
  let env;
  if (override) {
    env = override;
  } else {
    dotenv.config();
    env = process.env;
  }

  const result = schema.safeParse(env);

  if (!result.success) {
    console.log(JSON.stringify(result.error.format(), null, 2));
    throw new Error('Invalid or missing environment variables');
  }

  if (result.data.DEV_MODE_DO_NOT_ENABLE_IN_PRODUCTION_OR_YOU_WILL_BE_FIRED) {
    console.log('DEV MODE. DO NOT ENABLE IN PRODUCTION');
  }

  return result.data;
};

export const collectEnvironmentVariables = <T extends t.ZodRawShape>(schema: T, override?: Record<string, any>) => {
  return collectEnvironmentVariablesFromSchema(t.object(schema).and(DefaultSchema), override);
};

export const type = {
  string,
  number,
  boolean,
  list
};
