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
        compose_file = REPO_ROOT / "docker-compose.yml"
        woodpecker_file = REPO_ROOT / ".woodpecker.yml"
        readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
        compose = compose_file.read_text(encoding="utf-8") if compose_file.exists() else ""
        woodpecker = woodpecker_file.read_text(encoding="utf-8") if woodpecker_file.exists() else ""

        self.assertTrue(dockerfile.exists())
        self.assertTrue(dockerignore.exists())
        self.assertTrue(entrypoint.exists())
        self.assertTrue(compose_file.exists())
        self.assertTrue(woodpecker_file.exists())
        self.assertIn("docker build", readme)
        self.assertIn("docker run", readme)
        self.assertIn("docker compose up", readme)
        self.assertIn("gitea.chadlee.org/crenshaw/macro-sentiment-engine", readme)
        self.assertIn("gitea_registry_username", readme)
        self.assertIn("gitea_registry_password", readme)
        self.assertIn("gitea.chadlee.org/crenshaw/macro-sentiment-engine:latest", compose)
        self.assertIn("woodpeckerci/plugin-docker-buildx", woodpecker)
        self.assertIn("gitea.chadlee.org/${CI_REPO_OWNER}/${CI_REPO_NAME}", woodpecker)


if __name__ == "__main__":
    unittest.main()
