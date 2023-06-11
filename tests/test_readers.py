import datetime
from unittest import TestCase

from filemeta.fsreaders import read_dir
from filemeta.listreaders import iter_rar, iter_zip
from genutility.filesystem import FileProperties
from rarfile import nsdatetime

test_data = "test-data"


class FileTests(TestCase):
    def test_read_dir(self):
        # with self.assertNoLogs():
        a = read_dir(test_data)
        b = read_dir(test_data)
        self.assertEqual(a, b)

    def test_zip(self):
        result = list(iter_zip(test_data + "/test.zip", assume_utc=True))
        truth = [
            FileProperties(
                relpath="test.txt",
                size=3,
                isdir=False,
                modtime=datetime.datetime(2020, 9, 10, 15, 34, 14, tzinfo=datetime.timezone.utc),
                hash=4170839921,
            )
        ]
        self.assertEqual(truth, result)

    def test_rar(self):
        result = list(iter_rar(test_data + "/test.rar"))
        truth = [
            FileProperties(
                relpath="test.txt",
                size=3,
                isdir=False,
                modtime=nsdatetime(
                    2020,
                    9,
                    10,
                    7,
                    34,
                    15,
                    nanosecond=878340600,
                    tzinfo=datetime.timezone.utc,
                ),
                hash=4170839921,
            )
        ]
        self.assertEqual(truth, result)


if __name__ == "__main__":
    import unittest

    unittest.main()
