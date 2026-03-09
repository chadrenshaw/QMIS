# QMIS CI Deploy Script Implementation Plan

**Goal:** Add a safe remote deploy script for the published QMIS container and wire Woodpecker CI to upload the runtime assets and execute the deploy on `infra-docker.zocalo.net`.

**Architecture:** Keep QMIS on the existing single-container deployment model. Extend `.woodpecker.yaml` so pushes to `dev` can upload `docker-compose.yml` and the deploy script into `/srv/docker/qmis` on the remote Docker host, then execute the script over SSH. The script should pull the published image, restart only the `qmis` service, and fail fast if the service does not return to a running state.

**Tech Stack:** Bash, Docker Compose, Woodpecker CI, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Lock The CI Contract With Tests

**Files:**
- Modify: `tests/test_deployment_assets.py`

**Steps:**
1. Extend the deployment-asset test to require a QMIS deploy script in `docker/`.
2. Assert the script targets `/srv/docker/qmis` and deploys the `qmis` service with Docker Compose.
3. Assert `.woodpecker.yaml` uploads both `docker-compose.yml` and the deploy script to `infra-docker.zocalo.net` via SSH-driven CI steps.

### Task 2: Implement Remote Deployment Assets

**Files:**
- Create: `docker/deploy.sh`
- Modify: `.woodpecker.yaml`

**Steps:**
1. Add a bash deploy script modeled on the DYS safe deploy pattern, adapted for the single `qmis` service.
2. Add Woodpecker SCP and SSH steps that copy the compose file and deploy script into `/srv/docker/qmis` and then execute the script on `dev` pushes after the image publish step.
3. Reuse the existing registry credentials and remote-host SSH secrets instead of inventing a second deploy path.

### Task 3: Document Runtime And Secret Requirements

**Files:**
- Modify: `README.md`

**Steps:**
1. Document the remote deploy flow and the required Woodpecker secrets for host access.
2. Keep the container runtime instructions aligned with the new CI behavior and remote working directory.

### Task 4: Verification And Delivery

**Steps:**
1. Run `uv run python -m unittest tests.test_deployment_assets -v`.
2. Run `uv run python -m unittest -v`.
3. Commit and push the verified changes to `origin/dev`.
