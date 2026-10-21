FROM node:24.22.0-trixie AS builder
WORKDIR /app

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

COPY modules/module-core/package.json modules/module-core/tsconfig.json modules/module-core/
COPY modules/module-postgres/package.json modules/module-postgres/tsconfig.json modules/module-postgres/
COPY modules/module-postgres-storage/package.json modules/module-postgres-storage/tsconfig.json modules/module-postgres-storage/
COPY modules/module-mongodb/package.json modules/module-mongodb/tsconfig.json modules/module-mongodb/
COPY modules/module-mongodb-storage/package.json modules/module-mongodb-storage/tsconfig.json modules/module-mongodb-storage/
COPY modules/module-mysql/package.json modules/module-mysql/tsconfig.json modules/module-mysql/
COPY modules/module-mssql/package.json modules/module-mssql/tsconfig.json modules/module-mssql/
COPY modules/module-convex/package.json modules/module-convex/tsconfig.json modules/module-convex/

RUN corepack enable pnpm && corepack install
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

COPY modules/module-core/src modules/module-core/src/
COPY modules/module-postgres/src modules/module-postgres/src/
COPY modules/module-postgres/sql modules/module-postgres/sql/
COPY modules/module-postgres-storage/src modules/module-postgres-storage/src/
COPY modules/module-mongodb/src modules/module-mongodb/src/
COPY modules/module-mongodb-storage/src modules/module-mongodb-storage/src/
COPY modules/module-mysql/src modules/module-mysql/src/
COPY modules/module-mssql/src modules/module-mssql/src/
COPY modules/module-convex/src modules/module-convex/src/

RUN pnpm build:production && \
    rm -rf node_modules **/node_modules && \
    pnpm install --frozen-lockfile --prod --ignore-scripts

# === PROD ===

FROM node:24.22.0-trixie-slim
WORKDIR /app

COPY --from=builder /app/ ./

ARG SHA
ENV SHA=${SHA}
ENV NODE_ENV=production

RUN addgroup --system --gid 901 web && \
    adduser --gecos "" --disabled-password --uid 901 --ingroup web --home /home/web web

# Install the latest system CA certificates.
# Require the version fixing the October 2026 eMudhra compromise.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && dpkg --compare-versions \
        "$(dpkg-query -W -f='${Version}' ca-certificates)" \
        ge '20250419+deb13u1' \
    && rm -rf /var/lib/apt/lists/*

# Download the latest Mozilla CA bundle.
# Source: https://curl.se/docs/caextract.html
# Mozilla CA bundle, updated 2026-10-20.
# This intentionally fails the build if the upstream bundle is updated.
# At that point, it would be better to revert these changes and use an upstream image with fixed bundles.
ADD --chmod=644 --checksum=sha256:796f1389bfea13c0ed78f663b765baaff54a508e5483786bd6ece73c5e3e3cbe \
    https://curl.se/ca/cacert.pem \
    /etc/ssl/mozilla-cacert.pem

# SSL_CERT_DIR points to an empty directory to prevent OpenSSL from loading additional system trust anchors.
RUN mkdir -p /opt/empty-ca-dir
# Use the updated Mozilla bundle instead of Node.js's bundled CAs.
ENV SSL_CERT_FILE="/etc/ssl/mozilla-cacert.pem" \
    SSL_CERT_DIR="/opt/empty-ca-dir"


# Kaniko complains that /workspace already exists, not sure why.
RUN mkdir -p /workspace && chown -R web:web /workspace
RUN mkdir -p .probes && chown -R web:web .probes

USER 901

# Specifically avoid using NODE_OPTIONS for --use-openssl-ca, since some environments may override it.
ENTRYPOINT ["node", "--use-openssl-ca", "service/lib/entry.js"]
# This starts the unified service by default. 
CMD ["start"]
