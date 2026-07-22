CREATE TABLE public.large_rows (
  id bigint PRIMARY KEY,
  owner_id text NOT NULL,
  payload_base64 text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Each payload is exactly 6 MiB of valid base64 text. The decoded contents are
-- deliberately repetitive: the reproduction cares about API buffering and
-- fan-out, not source-side compression or random-data generation.
INSERT INTO public.large_rows (id, owner_id, payload_base64)
SELECT
  row_number,
  'shared-test-user',
  repeat('QUJD', (6 * 1024 * 1024) / 4)
FROM generate_series(1, 14) AS series(row_number);

CREATE PUBLICATION powersync FOR TABLE public.large_rows;
