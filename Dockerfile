FROM rust:latest as builder

WORKDIR /usr/src/md5rs-server

COPY . .

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/* && cargo build -r

FROM nvcr.io/nvidia/tensorrt:24.07-py3 as runtime

RUN apt-get update && apt-get install -y \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/md5rs-server/target/release/md5rs-server .
COPY --from=builder /usr/src/md5rs-server/target/release/*.so .
COPY ./start.sh .

RUN chmod +x /app/start.sh

# RUN ls -la /app

EXPOSE 50051

ENTRYPOINT ["/app/start.sh"]