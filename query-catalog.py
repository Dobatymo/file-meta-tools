import csv
from operator import itemgetter
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List, Optional

from filemeta.fsreaders import FilesDB
from filemeta.utils import DEFAULT_DB_PATH, signed_to_unsigned_int_64
from genutility.datetime import datetime_from_utc_timestamp_ns
from genutility.exceptions import assert_choices
from genutility.file import StdoutFile

if TYPE_CHECKING:
    from filemeta.fsreaders import FilesTuple
    from genutility.filesystem import PathType

fields = {
    "device": 0,
    "inode": 1,
    "root": 2,
    "path": 3,
    "filesize": 4,
    "mtime": 5,
}


def dict_to_csv(files, path=None, sortby=None):
    # type: (Iterable[FilesTuple], Optional[PathType], Optional[List[str]]) -> None

    assert_choices("sortby", sortby, fields.keys(), optional=True)

    with StdoutFile(path, "wt", encoding="utf-8", newline="") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields.keys())

        if not sortby:
            it = files
        else:
            it = sorted(files, key=itemgetter(*[fields[f] for f in sortby]))

        for device, inode, root, path, filesize, utcmtime in it:
            device = signed_to_unsigned_int_64(device)
            inode = signed_to_unsigned_int_64(inode)
            dt = datetime_from_utc_timestamp_ns(utcmtime, aslocal=True)
            csvwriter.writerow([device, inode, root, path, filesize, dt])


if __name__ == "__main__":
    from argparse import ArgumentParser

    from genutility.args import is_dir

    parser = ArgumentParser()
    parser.add_argument("action", choices=("export-current",))
    parser.add_argument("--drive", type=is_dir, help="Limit query to this drive")
    parser.add_argument("--db-path", type=str, default=DEFAULT_DB_PATH, help="Path to database file")
    parser.add_argument("--out", type=Path, help="Optional output file")
    parser.add_argument(
        "--sort-by",
        metavar="COL",
        nargs="+",
        choices=fields.keys(),
        help="Sort output by column",
    )
    parser.add_argument(
        "--deleted",
        action="store_true",
        help="Return deleted files instead of existing ones",
    )
    args = parser.parse_args()

    db = FilesDB(args.db_path)

    if args.action == "export-current":

        if args.drive:
            it = db.get_by_root(args.drive, args.deleted)
        else:
            it = db.get(args.deleted)

        dict_to_csv(it, args.out, args.sort_by)
