import json
import os
import time
from typing import Union

import requests


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _maybe(name: str) -> str:
    return os.getenv(name, "").strip()


def _ssh_key_ref() -> Union[int, str]:
    value = _require("DO_SSH_KEY_ID")
    return int(value) if value.isdigit() else value


def _build_cloud_init(env: dict) -> str:
    # Keep this simple and explicit.
    # Secrets are injected directly into files on the droplet, like your GH Action did.
    # The droplet will self-delete when done.
    return f"""#cloud-config
package_update: true
packages:
  - ca-certificates
  - curl

write_files:
  - path: /opt/job/ghcr.env
    permissions: "0600"
    content: |
      GHCR_USERNAME={env.get("GHCR_USERNAME", "")}
      GHCR_TOKEN={env.get("GHCR_TOKEN", "")}
      DO_TOKEN={env.get("DO_TOKEN", "")}

  - path: /opt/job/env
    permissions: "0600"
    content: |
      TZ={env.get("TZ", "")}
      EIA_API_KEY={env.get("EIA_API_KEY", "")}
      MONGO_URI={env.get("MONGO_URI", "")}
      MONGO_DB={env.get("MONGO_DB", "")}
      SPLASH_URL=http://splash:8050
      SPACES_KEY={env.get("SPACES_KEY", "")}
      SPACES_SECRET={env.get("SPACES_SECRET", "")}
      SPACES_BUCKET={env.get("SPACES_BUCKET", "")}
      SPACES_REGION={env.get("SPACES_REGION", "")}
      SPACES_ENDPOINT={env.get("SPACES_ENDPOINT", "")}
      TO_ADDRESSES={env.get("TO_ADDRESSES", "")}
      FROM_ADDRESS={env.get("FROM_ADDRESS", "")}
      AWS_SES_REGION_NAME={env.get("AWS_SES_REGION_NAME", "")}
      AWS_SES_ACCESS_KEY_ID={env.get("AWS_SES_ACCESS_KEY_ID", "")}
      AWS_SES_SECRET_ACCESS_KEY={env.get("AWS_SES_SECRET_ACCESS_KEY", "")}

  - path: /opt/job/run.sh
    permissions: "0700"
    content: |
      #!/usr/bin/env bash
      set -euo pipefail

      LOG=/var/log/job.log
      mkdir -p /opt/job
      echo "=== Job start: $(date -Is) ===" | tee -a "$LOG"

      set -a
      source /opt/job/env
      set +a

      if [ -f /opt/job/ghcr.env ]; then
        set -a
        source /opt/job/ghcr.env
        set +a
      fi

      echo "Installing Docker..." | tee -a "$LOG"
      curl -fsSL https://get.docker.com | sh >>"$LOG" 2>&1
      systemctl enable --now docker >>"$LOG" 2>&1

      if [ -n "${{GHCR_USERNAME:-}}" ] && [ -n "${{GHCR_TOKEN:-}}" ]; then
        echo "${{GHCR_TOKEN}}" | docker login ghcr.io -u "${{GHCR_USERNAME}}" --password-stdin >>"$LOG" 2>&1
      fi

      SPLASH_IMAGE="scrapinghub/splash:3.5"
      NET="jobnet"

      docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >>"$LOG" 2>&1

      echo "Pulling Splash image..." | tee -a "$LOG"
      docker pull "$SPLASH_IMAGE" >>"$LOG" 2>&1

      docker rm -f splash >/dev/null 2>&1 || true
      docker run -d --name splash --network "$NET" "$SPLASH_IMAGE" >>"$LOG" 2>&1

      echo "Waiting for Splash..." | tee -a "$LOG"
      for i in $(seq 1 60); do
        if docker run --rm --network "$NET" curlimages/curl:8.5.0 -fsS http://splash:8050/_ping >/dev/null 2>&1; then
          echo "Splash ready." | tee -a "$LOG"
          break
        fi
        sleep 2
      done

      echo "Pulling job image..." | tee -a "$LOG"
      docker pull "{env.get("JOB_IMAGE", "")}" >>"$LOG" 2>&1

      set +e
      docker run --rm --network "$NET" \
        --env-file /opt/job/env \
        -e SPLASH_URL=http://splash:8050 \
        "{env.get("JOB_IMAGE", "")}" /app/scheduler/run.sh 2>&1 | tee -a "$LOG"
      EXIT_CODE=${{PIPESTATUS[0]}}
      set -e

      echo "$EXIT_CODE" > /opt/job/exit_code
      echo "=== Job end: $(date -Is) exit=$EXIT_CODE ===" | tee -a "$LOG"

      # Self-delete droplet
      META=http://169.254.169.254/metadata/v1
      DROPLET_ID=$(curl -fsS $META/id || true)

      if [ -n "$DROPLET_ID" ] && [ -n "${{DO_TOKEN:-}}" ]; then
        echo "Deleting droplet $DROPLET_ID" | tee -a "$LOG"
        curl -fsS -X DELETE \
          -H "Authorization: Bearer $DO_TOKEN" \
          "https://api.digitalocean.com/v2/droplets/$DROPLET_ID" >>"$LOG" 2>&1 || true
      fi

      sync
      poweroff || true

runcmd:
  - [ bash, -lc, "/opt/job/run.sh" ]
"""


def main(event, context):
    do_token = _require("DO_TOKEN")
    do_api = os.getenv("DO_API", "https://api.digitalocean.com/v2")

    droplet_name = f"job-{int(time.time())}"

    region = _require("DO_REGION")
    size = _require("DO_SIZE")
    image = _require("DO_IMAGE")
    job_image = _require("JOB_IMAGE")
    tag = _require("DO_TAG")
    app_tag = _require("DO_APP_TAG")
    ssh_key_id = _ssh_key_ref()

    env_payload = {
        "TZ": _maybe("TZ"),
        "GHCR_USERNAME": _maybe("GHCR_USERNAME"),
        "GHCR_TOKEN": _maybe("GHCR_TOKEN"),
        "DO_TOKEN": do_token,
        "EIA_API_KEY": _maybe("EIA_API_KEY"),
        "MONGO_URI": _maybe("MONGO_URI"),
        "MONGO_DB": _maybe("MONGO_DB"),
        "SPACES_KEY": _maybe("SPACES_KEY"),
        "SPACES_SECRET": _maybe("SPACES_SECRET"),
        "SPACES_BUCKET": _maybe("SPACES_BUCKET"),
        "SPACES_REGION": _maybe("SPACES_REGION"),
        "SPACES_ENDPOINT": _maybe("SPACES_ENDPOINT"),
        "TO_ADDRESSES": _maybe("TO_ADDRESSES"),
        "FROM_ADDRESS": _maybe("FROM_ADDRESS"),
        "AWS_SES_REGION_NAME": _maybe("AWS_SES_REGION_NAME"),
        "AWS_SES_ACCESS_KEY_ID": _maybe("AWS_SES_ACCESS_KEY_ID"),
        "AWS_SES_SECRET_ACCESS_KEY": _maybe("AWS_SES_SECRET_ACCESS_KEY"),
        "JOB_IMAGE": job_image,
    }

    body = {
        "name": droplet_name,
        "region": region,
        "size": size,
        "image": image,
        "tags": [tag, app_tag],
        "ssh_keys": [ssh_key_id],
        "monitoring": True,
        "user_data": _build_cloud_init(env_payload),
    }

    vpc_uuid = _maybe("DO_VPC_UUID")
    if vpc_uuid and vpc_uuid.lower() != "null":
        body["vpc_uuid"] = vpc_uuid

    resp = requests.post(
        f"{do_api}/droplets",
        headers={
            "Authorization": f"Bearer {do_token}",
            "Content-Type": "application/json",
        },
        data=json.dumps(body),
        timeout=30,
    )
    resp.raise_for_status()

    data = resp.json()
    droplet = data.get("droplet", {})

    return {
        "body": {
            "ok": True,
            "droplet_id": droplet.get("id"),
            "droplet_name": droplet.get("name"),
            "status": droplet.get("status"),
        }
    }
