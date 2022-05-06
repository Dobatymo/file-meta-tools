import csv
import re
from functools import partial
from itertools import chain
from os import fspath
from pathlib import Path
from sys import stdout
from typing import IO, Callable, Dict, Iterable, Iterator, Optional

from filemeta.listreaders import (
    iter_archive,
    iter_archiveorg_xml,
    iter_dir,
    iter_gamedat_xml,
    iter_rar,
    iter_syncthing,
    iter_zip,
)
from filemeta.utils import HashableLessThan, iterable_to_dict_by_key
from genutility.args import existing_path
from genutility.file import StdoutFile
from genutility.filesdb import FileDbSimple, NoResult
from genutility.filesystem import FileProperties
from genutility.hash import crc32_hash_file, md5_hash_file, sha1_hash_file
from genutility.torrent import iter_fastresume, iter_torrent

""" current limitations: All paths on one side, need to be of the same type.
	Auto type detection will only use the first entry for type determination.

"""


def iter_torrents(path: str, hashfunc: Optional[str] = None, dirs: Optional[bool] = None) -> Iterator[FileProperties]:
    for file in Path(path).rglob("*.torrent"):
        yield from iter_torrent(fspath(file))


def iter_fastresumes(
    path: str, hashfunc: Optional[str] = None, dirs: Optional[bool] = None
) -> Iterator[FileProperties]:
    for file in Path(path).rglob("*.fastresume"):
        yield from iter_fastresume(fspath(file))


class HashDB(FileDbSimple):
    @classmethod
    def derived(cls):
        return [
            ("sha1", "CHAR(40)", "?"),
            ("md5", "CHAR(32)", "?"),
            ("crc32", "CHAR(8)", "?"),
        ]

    def __init__(self, path):
        FileDbSimple.__init__(self, path, "hashes")


class Hasher:
    def __init__(self, hash: Optional[str] = None, cache: Optional[Path] = None) -> None:
        self.hash = hash
        if cache:
            self.db: Optional[HashDB] = HashDB(cache)
        else:
            self.db = None

        if hash == "sha1":
            self.hashfunc = lambda path: sha1_hash_file(path).hexdigest()
        elif hash == "md5":
            self.hashfunc = lambda path: md5_hash_file(path).hexdigest()
        elif hash == "crc32":
            # self.hashfunc = lambda: int(crc32_hash_file(path), 16)
            self.hashfunc = crc32_hash_file
        else:

            def hashfunc(path):
                raise ValueError("Invalid hash function")

            self.hashfunc = hashfunc

    def get(self, path: Path) -> str:
        if self.db is not None:
            try:
                (value,) = self.db.get(path, only={self.hash})
            except NoResult:
                value = self.hashfunc(path)
                self.db.add(path, derived={self.hash: value})
        else:
            value = self.hashfunc(path)

        return value


def files_to_csv(files: Iterable[FileProperties], csvpath: str) -> None:

    with open(csvpath, "w", newline="", encoding="utf-8") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(FileProperties.keys())
        for props in files:
            csvwriter.writerow(props.values())


def compare(
    a: Dict[HashableLessThan, FileProperties],
    b: Dict[HashableLessThan, FileProperties],
    hasher: Optional[Hasher],
    left: bool = True,
    right: bool = True,
    both: bool = True,
    ignore: re.Pattern = None,
    file: IO[str] = stdout,
) -> None:

    aset = a.keys()
    bset = b.keys()

    # retleft = None
    # retright = None

    # note: the key is usually the `relpath` or the `hash`

    if left:
        print("In left only", file=file)
        for key in sorted(aset - bset):
            if ignore and ignore.match(fspath(key)):
                continue
            print("lo:", key, a[key].relpath, file=file)

    if right:
        print("In right only", file=file)
        for key in sorted(bset - aset):
            if ignore and ignore.match(fspath(key)):
                continue
            print("ro:", key, b[key].relpath, file=file)

    if both:
        print("On both, but different", file=file)
        for key in sorted(aset & bset):
            if ignore and ignore.match(fspath(key)):
                continue

            aprops = a[key]
            bprops = b[key]

            if aprops.isdir != bprops.isdir:
                print("bo:", "one is dir, one is file", key, file=file)
            if not aprops.isdir:
                if aprops.size != bprops.size:
                    print("bo:", "size different", key, aprops.size, bprops.size, file=file)
                elif aprops.size == 0 and bprops.size == 0:
                    pass
                elif hasher is not None:  # same size
                    if (aprops.hash or aprops.abspath) and (bprops.hash or bprops.abspath):
                        if not aprops.hash:
                            aprops.hash = hasher.get(Path(aprops.abspath))  # type: ignore [arg-type]
                        if not bprops.hash:
                            bprops.hash = hasher.get(Path(bprops.abspath))  # type: ignore [arg-type]
                        if aprops.hash != bprops.hash:
                            print("bo:", "hash different", key, aprops.hash, bprops.hash, file=file)
                        # else: pass # same files
                    else:
                        print("bo:", "no hash or abspath for same size files", key, file=file)


def find_type(path: Path) -> Callable:

    if path.is_dir():
        return iter_dir
    elif path.is_file():
        if path.suffix == ".torrent":
            return iter_torrent
        elif path.suffix in {"rar", "cbr"}:
            return iter_rar
        elif path.suffix in {"zip", "cbz"}:
            return iter_zip
        elif path.suffix == ".xml":
            return iter_archiveorg_xml
        elif path.suffix == ".dat":
            return iter_gamedat_xml

    raise RuntimeError("Type detection failed")


def funcname(f: Callable) -> str:

    if isinstance(f, partial):
        return f.func.__name__
    else:
        return f.__name__


if __name__ == "__main__":
    from argparse import ArgumentParser

    from appdirs import user_data_dir

    types = {
        "archive": iter_archive,
        "torrent": iter_torrent,
        "torrents": iter_torrents,
        "qb-fastresume": iter_fastresumes,
        "dir": iter_dir,
        "archiveorg_xml": iter_archiveorg_xml,
        "gamedat_xml": iter_gamedat_xml,
        "syncthing": iter_syncthing,
    }

    def path_or_url(path):
        if path.startswith(("http://", "https://")):
            return path
        return existing_path(path)

    DEFAULT_HASHCACHE = Path(user_data_dir("file-meta-tools", "Dobatymo")) / "hashes.sqlite"

    parser = ArgumentParser(
        description="Compare contents of two different file trees to each other. Various different sources, like local directories, archives and torrent files are supported."
    )
    parser.add_argument("--left-paths", nargs="+", type=path_or_url, required=True)
    parser.add_argument("--right-paths", nargs="+", type=path_or_url, required=True)
    parser.add_argument(
        "--left-rel",
        nargs="+",
        default=None,
        help="Top level directory of left path everything will be relative to",
    )
    parser.add_argument(
        "--right-rel",
        nargs="+",
        default=None,
        help="Top level directory of right path everything will be relative to",
    )
    parser.add_argument("--left-type", choices=types.keys(), default="auto", help="Type of left content")
    parser.add_argument(
        "--right-type",
        choices=types.keys(),
        default="auto",
        help="Type of right content",
    )
    parser.add_argument(
        "--by", choices={"relpath", "hash", "abspath"}, default="relpath", help="File property to use as the group key"
    )
    parser.add_argument("--hashfunc", choices=("sha1", "md5", "crc32"), default=None)
    parser.add_argument(
        "--no-dirs",
        action="store_true",
        help="Ingnore missing directories. Use for input types which don't have explicit directories, like torrent files.",
    )
    parser.add_argument("--no-print-left", action="store_true")
    parser.add_argument("--no-print-right", action="store_true")
    parser.add_argument("--no-print-both", action="store_true")
    parser.add_argument("--out-path", type=Path, help="Output file, default is stdout")
    parser.add_argument(
        "--hashcache",
        type=Path,
        default=DEFAULT_HASHCACHE,
        help="Path to sqlite database file to load/store cached hashes",
    )
    args = parser.parse_args()

    if args.left_rel is None:
        args.left_rel = ["."] * len(args.left_paths)
    elif len(args.left_rel) != len(args.left_paths):
        raise ValueError("Number of left relative paths doesn't match number of paths")

    if args.right_rel is None:
        args.right_rel = ["."] * len(args.right_paths)
    elif len(args.right_rel) != len(args.right_paths):
        raise ValueError("Number of right relative paths doesn't match number of paths")

    if args.left_type == "auto":
        iter_func_a = partial(find_type(args.left_paths[0]), dirs=not args.no_dirs)
    else:
        if args.hashfunc:
            iter_func_a = partial(types[args.left_type], hashfunc=args.hashfunc, dirs=not args.no_dirs)
        else:
            iter_func_a = partial(types[args.left_type], dirs=not args.no_dirs)

    if args.right_type == "auto":
        iter_func_b = partial(find_type(args.right_paths[0]), dirs=not args.no_dirs)
    else:
        if args.hashfunc:
            iter_func_b = partial(types[args.right_type], hashfunc=args.hashfunc, dirs=not args.no_dirs)
        else:
            iter_func_b = partial(types[args.right_type], dirs=not args.no_dirs)

    print("Using input functions:", funcname(iter_func_a), funcname(iter_func_b))

    # a = iterable_to_dict_by_key(iter_func_a(args.archive, args.topleveldir))

    if args.by in ("relpath", "abspath"):
        apply: Optional[Callable] = Path  # necessary for case-insensitive path compares under Windows
    else:
        apply = None
    a = iterable_to_dict_by_key(args.by, chain.from_iterable(map(iter_func_a, map(fspath, args.left_paths))), apply)
    b = iterable_to_dict_by_key(args.by, chain.from_iterable(map(iter_func_b, map(fspath, args.right_paths))), apply)

    hasher = Hasher(args.hashfunc, args.hashcache)

    with StdoutFile(args.out_path, "wt", encoding="utf-8") as fw:
        compare(
            a,
            b,
            hasher,
            not args.no_print_left,
            not args.no_print_right,
            not args.no_print_both,
            ignore=re.compile(r"^.*\.pyc$"),
            file=fw,
        )
