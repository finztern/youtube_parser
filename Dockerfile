FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# sing-box: local SOCKS inbounds for the subscription nodes (vless + hysteria2, nodes.py).
ARG SINGBOX_VERSION=1.10.7
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && case "$(dpkg --print-architecture)" in arm64) A=arm64 ;; *) A=amd64 ;; esac \
 && curl -fsSL "https://github.com/SagerNet/sing-box/releases/download/v${SINGBOX_VERSION}/sing-box-${SINGBOX_VERSION}-linux-${A}.tar.gz" \
  | tar -xz --strip-components=1 -C /usr/local/bin "sing-box-${SINGBOX_VERSION}-linux-${A}/sing-box" \
 && chmod +x /usr/local/bin/sing-box \
 && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY . .

CMD ["python3", "main.py"]
