FROM node:20.12.2 as builder
WORKDIR /app

RUN npm i -g pnpm@9

ARG GITHUB_TOKEN

RUN echo //npm.pkg.github.com/:_authToken=\${GITHUB_TOKEN} >> ~/.npmrc && \
    echo //npm.pkg.github.com/journeyapps-platform/:_authToken=\${GITHUB_TOKEN} >> ~/.npmrc && \
    echo @journeyapps-platform:registry=https://npm.pkg.github.com/journeyapps-platform/ >> ~/.npmrc

COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
COPY service/package.json service/tsconfig.json service/

COPY packages/jpgwire/package.json packages/jpgwire/tsconfig.json packages/jpgwire/
COPY packages/jsonbig/package.json packages/jsonbig/tsconfig.json packages/jsonbig/
COPY packages/rsocket-router/package.json packages/rsocket-router/tsconfig.json packages/rsocket-router/
COPY packages/service-core/package.json packages/service-core/tsconfig.json packages/service-core/
COPY packages/sync-rules/package.json packages/sync-rules/tsconfig.json packages/sync-rules/
COPY packages/types/package.json packages/types/tsconfig.json packages/types/

RUN pnpm install --frozen-lockfile

COPY service/src service/src/

COPY packages/service-core/src packages/service-core/src/
COPY packages/jpgwire/src packages/jpgwire/src/
COPY packages/jpgwire/ca packages/jpgwire/ca/
COPY packages/jsonbig/src packages/jsonbig/src/
COPY packages/sync-rules/src packages/sync-rules/src/
COPY packages/rsocket-router/src packages/rsocket-router/src/
COPY packages/types/src packages/types/src/

RUN pnpm build:production && \
    rm -rf node_modules **/node_modules && \
    pnpm install --frozen-lockfile --prod --ignore-scripts

# === TESTER ===

FROM node:20.12.2-slim as tester
WORKDIR /app

ARG GITHUB_TOKEN

COPY --from=builder /app/ ./
COPY --from=builder /root/.npmrc /root/.npmrc

RUN npm i -g pnpm@9

RUN pnpm install --frozen-lockfile && rm /root/.npmrc


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
