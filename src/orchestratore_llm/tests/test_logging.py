"""Il file di log ruota quando supera max_bytes."""

import logging
import tempfile
import unittest
from pathlib import Path

from orchestratore_llm.logging_config import configure_logging


class TestLogRotation(unittest.TestCase):
    def test_file_rotates_past_max_bytes(self) -> None:
        """Oltre max_bytes resta il file corrente e la copia .1."""
        with tempfile.TemporaryDirectory() as folder:
            destination = Path(folder) / "orch.log"
            logger = configure_logging(
                level=logging.INFO,
                log_file=destination,
                logger_name="test.rotazione.orch",
                max_bytes=400,
                backup_count=2,
            )
            try:
                for i in range(12):
                    logger.info("riga %s %s", i, "x" * 80)
            finally:
                for handler in list(logger.handlers):
                    handler.close()
                    logger.removeHandler(handler)

            self.assertTrue(destination.exists())
            self.assertTrue(destination.with_name("orch.log.1").exists())
            self.assertLessEqual(destination.stat().st_size, 400 * 3)


if __name__ == "__main__":
    unittest.main()
