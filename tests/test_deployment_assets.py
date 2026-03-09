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
        readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

        self.assertTrue(dockerfile.exists())
        self.assertTrue(dockerignore.exists())
        self.assertTrue(entrypoint.exists())
        self.assertTrue(compose_file.exists())
        self.assertIn("docker build", readme)
        self.assertIn("docker run", readme)
        self.assertIn("docker compose up", readme)


if __name__ == "__main__":
    unittest.main()
