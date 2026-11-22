FROM --platform=$BUILDPLATFORM rust:1.100-slim-bookworm AS build
ARG BUILDARCH
ARG TARGETARCH
WORKDIR /build

RUN if [ "$BUILDARCH" != "$TARGETARCH" ]; then \
      test "$TARGETARCH" = amd64 && \
      apt-get update && apt-get install -y --no-install-recommends gcc-x86-64-linux-gnu libc6-dev-amd64-cross && \
      rustup target add x86_64-unknown-linux-gnu; \
    fi

COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    if [ "$BUILDARCH" = "$TARGETARCH" ]; then \
      cargo build --locked --release && cp target/release/codec-shopping-sessions /shopping-sessions; \
    else \
      CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
        cargo build --locked --release --target x86_64-unknown-linux-gnu && \
      cp target/x86_64-unknown-linux-gnu/release/codec-shopping-sessions /shopping-sessions; \
    fi

FROM debian:bookworm-slim
COPY --from=build /shopping-sessions /usr/local/bin/shopping-sessions
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/shopping-sessions"]
