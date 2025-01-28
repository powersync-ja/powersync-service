FROM node:20.12.2 AS builder
WORKDIR /app

RUN npm i -g pnpm@9

COPY package.json pnpm-workspace.yaml pnpm-lock.yaml tsconfig.base.json ./
COPY service/package.json service/tsconfig.json service/

COPY packages/jpgwire/package.json packages/jpgwire/tsconfig.json packages/jpgwire/
COPY packages/jsonbig/package.json packages/jsonbig/tsconfig.json packages/jsonbig/
COPY packages/rsocket-router/package.json packages/rsocket-router/tsconfig.json packages/rsocket-router/
COPY packages/service-core/package.json packages/service-core/tsconfig.json packages/service-core/
COPY packages/service-errors/package.json packages/service-errors/tsconfig.json packages/service-errors/
COPY packages/sync-rules/package.json packages/sync-rules/tsconfig.json packages/sync-rules/
COPY packages/types/package.json packages/types/tsconfig.json packages/types/

COPY libs/lib-services/package.json libs/lib-services/tsconfig.json libs/lib-services/
COPY libs/lib-mongodb/package.json libs/lib-mongodb/tsconfig.json libs/lib-mongodb/
COPY libs/lib-postgres/package.json libs/lib-postgres/tsconfig.json libs/lib-postgres/

COPY modules/module-postgres/package.json modules/module-postgres/tsconfig.json modules/module-postgres/
COPY modules/module-postgres-storage/package.json modules/module-postgres-storage/tsconfig.json modules/module-postgres-storage/
COPY modules/module-mongodb/package.json modules/module-mongodb/tsconfig.json modules/module-mongodb/
COPY modules/module-mongodb-storage/package.json modules/module-mongodb-storage/tsconfig.json modules/module-mongodb-storage/
COPY modules/module-mysql/package.json modules/module-mysql/tsconfig.json modules/module-mysql/

RUN pnpm install --frozen-lockfile

COPY service/src service/src/

COPY packages/service-core/src packages/service-core/src/
COPY packages/service-errors/src packages/service-errors/src/
COPY packages/jpgwire/src packages/jpgwire/src/
COPY packages/jpgwire/ca packages/jpgwire/ca/
COPY packages/jsonbig/src packages/jsonbig/src/
COPY packages/sync-rules/src packages/sync-rules/src/
COPY packages/sync-rules/scripts packages/sync-rules/scripts/
COPY packages/rsocket-router/src packages/rsocket-router/src/
COPY packages/types/src packages/types/src/

COPY libs/lib-services/src libs/lib-services/src/
COPY libs/lib-mongodb/src libs/lib-mongodb/src/
COPY libs/lib-postgres/src libs/lib-postgres/src/

COPY modules/module-postgres/src modules/module-postgres/src/
COPY modules/module-postgres-storage/src modules/module-postgres-storage/src/
COPY modules/module-mongodb/src modules/module-mongodb/src/
COPY modules/module-mongodb-storage/src modules/module-mongodb-storage/src/
COPY modules/module-mysql/src modules/module-mysql/src/

RUN pnpm build:production && \
    rm -rf node_modules **/node_modules && \
    pnpm install --frozen-lockfile --prod --ignore-scripts

# === TESTER ===

FROM node:20.12.2-slim AS tester
WORKDIR /app


COPY --from=builder /app/ ./

RUN npm i -g pnpm@9

RUN pnpm install --frozen-lockfile


CMD pnpm test

# === PROD ===

FROM node:20.12.2-slim
WORKDIR /app

COPY --from=builder /app/ ./

ARG SHA
ENV SHA=${SHA}
ENV NODE_ENV=production

RUN addgroup --system --gid 901 web && \
    adduser --gecos "" --disabled-password --uid 901 --ingroup web --home /home/web web

# Kaniko complains that /workspace already exists, not sure why.
RUN mkdir -p /workspace && chown -R web:web /workspace
RUN mkdir -p .probes && chown -R web:web .probes

USER 901

ENTRYPOINT ["node", "service/lib/entry.js"]
# This starts the unified service by default. 
CMD ["start"]
