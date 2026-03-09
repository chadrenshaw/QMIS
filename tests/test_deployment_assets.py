import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISDeploymentAssetTests(unittest.TestCase):
    def test_deployment_assets_exist_and_readme_documents_runtime_commands(self) -> None:
        dockerfile = REPO_ROOT / "Dockerfile"
        dockerignore = REPO_ROOT / ".dockerignore"
        entrypoint = REPO_ROOT / "docker" / "entrypoint.sh"
        deploy_script = REPO_ROOT / "docker" / "deploy.sh"
        compose_file = REPO_ROOT / "docker-compose.yml"
        woodpecker_file = REPO_ROOT / ".woodpecker.yaml"
        readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
        compose = compose_file.read_text(encoding="utf-8") if compose_file.exists() else ""
        woodpecker = woodpecker_file.read_text(encoding="utf-8") if woodpecker_file.exists() else ""
        deploy = deploy_script.read_text(encoding="utf-8") if deploy_script.exists() else ""

        self.assertTrue(dockerfile.exists())
        self.assertTrue(dockerignore.exists())
        self.assertTrue(entrypoint.exists())
        self.assertTrue(deploy_script.exists())
        self.assertTrue(compose_file.exists())
        self.assertTrue(woodpecker_file.exists())
        self.assertIn("docker build", readme)
        self.assertIn("docker run", readme)
        self.assertIn("docker compose up", readme)
        self.assertIn("https://gitea.chadlee.org/crenshaw/QMIS", readme)
        self.assertIn("gitea_registry_username", readme)
        self.assertIn("gitea_registry_password", readme)
        self.assertIn("gitea.chadlee.org/crenshaw/qmis:latest", readme)
        self.assertIn("gitea.chadlee.org/crenshaw/qmis:latest", compose)
        self.assertIn("woodpeckerci/plugin-docker-buildx", woodpecker)
        self.assertIn("repo: gitea.chadlee.org/${CI_REPO_OWNER}/qmis", woodpecker)
        self.assertIn("/srv/docker/qmis", deploy)
        self.assertIn("docker compose -f \"${COMPOSE_FILE}\" pull qmis", deploy)
        self.assertIn("docker compose -f \"${COMPOSE_FILE}\" up -d --no-deps qmis", deploy)
        self.assertIn("appleboy/drone-scp", woodpecker)
        self.assertIn("appleboy/drone-ssh", woodpecker)
        self.assertIn("target: /srv/docker/qmis/", woodpecker)
        self.assertIn("source: docker/deploy.sh", woodpecker)
        self.assertIn("/srv/docker/qmis/deploy.sh", woodpecker)
        self.assertIn("DEV_DOCKER_HOST_IP", woodpecker)
        self.assertIn("DOCKER_HOST_USER", woodpecker)
        self.assertIn("DOCKER_HOST_SSH_KEY", woodpecker)
        self.assertIn("/srv/docker/qmis", readme)
        self.assertIn("infra-docker.zocalo.net", readme)


if __name__ == "__main__":
    unittest.main()
