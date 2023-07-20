import tempfile
import zipfile
from pathlib import Path
from typing import Optional
from unittest import TestCase

from filemetatools import zip

test_data = Path("test-data")


def test_zip(path: str) -> Optional[str]:
    with zipfile.ZipFile(path, "r") as zf:
        return zf.testzip()


class ZipTests(TestCase):
    def test_copy_zip(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp = Path(tmpdirname)
            for name in ["7zip22.zip", "winrar6.zip"]:
                for extras in [True, False]:
                    with self.subTest(name=name, extras=extras):
                        out = tmp / name
                        zip.copy_zip(test_data / name, out, extras=extras, comments=True)
                        self.assertIsNone(test_zip(out))
