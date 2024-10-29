<p align="center">
  <a href="https://www.powersync.com" target="_blank"><img src="https://github.com/powersync-ja/.github/assets/7372448/d2538c43-c1a0-4c47-9a76-41462dba484f"/></a>
</p>

*[PowerSync](https://www.powersync.com) is a sync engine for building local-first apps with instantly-responsive UI/UX and simplified state transfer. Syncs between SQLite on the client-side and Postgres, MongoDB or MySQL on the server-side.*

# Quick reference

Where to get help:

- [Discord](https://discord.gg/powersync)
- [Email](mailto:support@powersync.com)

Where to file issues:
https://github.com/powersync-ja/powersync-service/issues

Supported architectures: ([more info](https://github.com/docker-library/official-images#architectures-other-than-amd64))
amd64, arm64

Source of this description:
[Github link](https://github.com/powersync-ja/powersync-service/blob/main/service/README.md)

# How to use this image

Start a PowerSync server instance

```bash
docker run \
-p 8080:80 \
-e POWERSYNC_CONFIG_B64="$(base64 -i ./config.yaml)" \
--network my-local-dev-network \
--name my-powersync journeyapps/powersync-service:latest
```

See the [documentation](https://docs.powersync.com/self-hosting/installation) for additional information.

See the [Docker Compose Demo](https://github.com/powersync-ja/self-host-demo) for a Docker Compose quick start.

# Telemetry

PowerSync may collect telemetry data about your usage. Telemetry data allows us to shape our roadmap to better serve you and other customers. Collection of telemetry data is anonymized and optional. If you do not want telemetry data collected, you may opt-out. See https://docs.powersync.com/self-hosting/telemetry for further info.

# License

This image is published under the [FSL-1.1-Apache-2.0](https://www.powersync.com/legal/fsl) license.

View [license information](https://github.com/powersync-ja/powersync-service/blob/main/service/LICENSE) for the software contained in this image.

As with all Docker images, these likely also contain other software which may be under other licenses (such as Bash, etc from the base distribution, along with any direct or indirect dependencies of the primary software being contained).

As for any pre-built image usage, it is the image user's responsibility to ensure that any use of this image complies with any relevant licenses for all software contained within.