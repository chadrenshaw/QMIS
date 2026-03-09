import json
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WEB_ROOT = REPO_ROOT / "web"


class QMISWebScaffoldTests(unittest.TestCase):
    def test_web_scaffold_files_and_documented_commands_exist(self) -> None:
        required_files = [
            WEB_ROOT / "package.json",
            WEB_ROOT / "vite.config.ts",
            WEB_ROOT / "tailwind.config.ts",
            WEB_ROOT / "postcss.config.js",
            WEB_ROOT / "src" / "main.tsx",
            WEB_ROOT / "src" / "App.tsx",
            WEB_ROOT / "src" / "index.css",
            WEB_ROOT / "src" / "api" / "client.ts",
            WEB_ROOT / "src" / "api" / "queries.ts",
        ]

        missing = [str(path.relative_to(REPO_ROOT)) for path in required_files if not path.exists()]
        self.assertFalse(missing, msg=f"Missing web scaffold files: {missing}")

        package_json = json.loads((WEB_ROOT / "package.json").read_text())
        scripts = package_json.get("scripts", {})
        self.assertIn("dev", scripts)
        self.assertIn("build", scripts)
        self.assertIn("test", scripts)

        dependencies = package_json.get("dependencies", {})
        dev_dependencies = package_json.get("devDependencies", {})
        self.assertIn("react", dependencies)
        self.assertIn("@tanstack/react-query", dependencies)
        self.assertIn("recharts", dependencies)
        self.assertIn("tailwindcss", dev_dependencies)
        self.assertIn("vite", dev_dependencies)

        readme = (REPO_ROOT / "README.md").read_text()
        self.assertIn("npm install", readme)
        self.assertIn("npm run dev", readme)
        self.assertIn("npm run build", readme)


if __name__ == "__main__":
    unittest.main()
