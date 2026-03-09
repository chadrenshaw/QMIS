import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISNewsCollectorPlaceholderTests(unittest.TestCase):
    def test_news_collector_placeholder_raises_clear_not_configured_error(self) -> None:
        from qmis.collectors.news import NewsCollectorNotConfiguredError, run_news_collector

        with self.assertRaises(NewsCollectorNotConfiguredError) as context:
            run_news_collector()

        message = str(context.exception)
        self.assertIn("news collector", message.lower())
        self.assertIn("source/provider", message.lower())
        self.assertIn("authoritative docs", message.lower())

    def test_news_collector_placeholder_metadata_is_explicitly_blocked(self) -> None:
        from qmis.collectors.news import NEWS_COLLECTOR_STATUS

        self.assertEqual(NEWS_COLLECTOR_STATUS["status"], "blocked")
        self.assertEqual(NEWS_COLLECTOR_STATUS["configured"], False)
        self.assertIn("no source/provider is specified", NEWS_COLLECTOR_STATUS["reason"])


if __name__ == "__main__":
    unittest.main()
