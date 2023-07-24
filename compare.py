import csv
import logging
import os.path
import re
import shutil
import sys
from collections import defaultdict
from functools import partial
from itertools import chain
from os import fspath
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Mapping, Optional, TypeVar

import xxhash
from filemeta.listreaders import (
    iter_archive,
    iter_archiveorg_xml,
    iter_dir,
    iter_gamedat_xml,
    iter_rar,
    iter_syncthing,
    iter_zip,
)
from filemeta.utils import HashableLessThan, SmartPath, iterable_to_dict_by_key
from genutility.args import existing_path
from genutility.file import StdoutFile
from genutility.filesdb import FileDbSimple, NoResult
from genutility.filesystem import FileProperties
from genutility.hash import crc32_hash_file, hash_file, md5_hash_file, sha1_hash_file
from genutility.torrent import iter_fastresume
from genutility.torrent import iter_torrent as _iter_torrent
from rich.progress import Progress, TaskProgressColumn, TextColumn, TimeElapsedColumn
from send2trash import send2trash

""" current limitations: All paths on one side, need to be of the same type.
	Auto type detection will only use the first entry for type determination.

"""

xxhash_hash_file = partial(hash_file, hashcls=xxhash.xxh3_128)


def iter_torrent(path: str, dirs: Optional[bool] = None) -> Iterator[FileProperties]:
    return _iter_torrent(SmartPath(path))


def iter_torrents(path: str, hashfunc: Optional[str] = None, dirs: Optional[bool] = None) -> Iterator[FileProperties]:
    for file in Path(path).rglob("*.torrent"):
        yield from iter_torrent(fspath(file))


def iter_fastresumes(
    path: str, hashfunc: Optional[str] = None, dirs: Optional[bool] = None
) -> Iterator[FileProperties]:
    for file in Path(path).rglob("*.fastresume"):
        yield from iter_fastresume(file)


class HashDB(FileDbSimple):
    @classmethod
    def derived(cls):
        return [
            ("sha1", "CHAR(40)", "?"),
            ("md5", "CHAR(32)", "?"),
            ("crc32", "CHAR(8)", "?"),
        ]

    def __init__(self, path: str) -> None:
        FileDbSimple.__init__(self, path, "hashes")


class Hasher:
    hash: Optional[str]
    hashfunc: Optional[Callable[[Path], str]]

    def __init__(self, hash: Optional[str] = None, cache: Optional[Path] = None) -> None:
        self.hash = hash
        if cache:
            cache.parent.mkdir(parents=True, exist_ok=True)
            self.db: Optional[HashDB] = HashDB(fspath(cache))
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
            self.hashfunc = None

    def get(self, path: Path) -> str:
        if self.hash is None or self.hashfunc is None:
            raise ValueError("Hash function not specified")

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


class BaseAction:
    def __init__(self, types: List[str], **kwargs) -> None:
        pass

    def do_single(self, type_: str, key: HashableLessThan, value: FileProperties) -> None:
        raise NotImplementedError

    def do_both(self, type_: str, key: HashableLessThan, left: FileProperties, right: FileProperties) -> None:
        raise NotImplementedError

    def close(self) -> None:
        pass


class IgnoreAction(BaseAction):
    def do_single(self, type_: str, key: HashableLessThan, value: FileProperties) -> None:
        pass

    def do_both(self, type_: str, key: HashableLessThan, left: FileProperties, right: FileProperties) -> None:
        pass


class PrintAction(BaseAction):
    def __init__(self, types: List[str], **kwargs) -> None:
        super().__init__(types)
        self.stdfile = StdoutFile(kwargs["out_path"], "wt", encoding="utf-8")
        self.file = self.stdfile.fp

    def do_single(self, type_: str, key: HashableLessThan, value: FileProperties) -> None:
        print(f"{type_}:", key, value.relpath, file=self.file)

    def do_both(self, type_: str, key: HashableLessThan, left: FileProperties, right: FileProperties) -> None:
        if type_ == "same_file":
            print("both:", "paths refer to the same file", key, left.abspath, right.abspath)
        elif type_ == "different_type":
            print(f"both: one is dir, one is file `{key}` isdir={left.isdir}, isdir={right.isdir}", file=self.file)
        elif type_ == "directory":
            print("both:", "entries are dictionaries", key, file=self.file)
        elif type_ == "different_size":
            print("both:", "size different", key, left.size, right.size, file=self.file)
        elif type_ == "empty":
            print("both:", "files are empty", key, file=self.file)
        elif type_ == "missing_hash":
            print("both:", "no hash or abspath for same size files", key, file=self.file)
        elif type_ == "same_size_no_hash":
            print("both:", "same size files with not hash comparison required", key, file=self.file)
        elif type_ == "same_hash":
            print("both:", "hashes are same", key, file=self.file)
        elif type_ == "different_hash":
            print("both:", "hashes are different", key, left.hash, right.hash, file=self.file)
        else:
            raise ValueError(type_)

    def close(self):
        self.stdfile.close()


class CopyAction(BaseAction):
    def __init__(self, types: List[str], **kwargs) -> None:
        super().__init__(types)
        self.dst = kwargs["copy_dst"]
        assert isinstance(self.dst, Path)

    def do_single(self, type_: str, key: HashableLessThan, value: FileProperties) -> None:
        assert value.relpath is not None
        assert value.abspath is not None

        src = value.abspath
        dst = self.dst / value.relpath
        print(f"[{type_}] Copying {src} to {dst}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)


class TrashAction(BaseAction):
    def do_single(self, type_: str, key: HashableLessThan, value: FileProperties) -> None:
        assert value.relpath is not None
        assert value.abspath is not None
        print(f"[{type_}] Trashing {value.abspath}")
        send2trash(value.abspath)


ACTIONS_BOTH = [
    "different_type",
    "directory",
    "different_size",
    "empty",
    "missing_hash",
    "same_size_no_hash",
    "same_hash",
    "different_hash",
]


def compare(
    a: Dict[HashableLessThan, FileProperties],
    b: Dict[HashableLessThan, FileProperties],
    hasher: Optional[Hasher],
    actions: Mapping[str, Optional[BaseAction]],
    by: Optional[str] = None,
    ignore: Optional[re.Pattern] = None,
) -> None:
    aset = a.keys()
    bset = b.keys()

    # retleft = None
    # retright = None

    # note: the key is usually the `relpath` or the `hash`

    action_left_only = actions["left_only"]
    action_right_only = actions["right_only"]

    if action_left_only is not None:
        for key in sorted(aset - bset):
            if ignore and by in ("relpath", "abspath") and ignore.match(fspath(key)):
                continue
            action_left_only.do_single("left_only", key, a[key])

    if action_right_only is not None:
        for key in sorted(bset - aset):
            if ignore and by in ("relpath", "abspath") and ignore.match(fspath(key)):
                continue
            action_right_only.do_single("right_only", key, b[key])

    if any(actions.get(a, None) is not None for a in ACTIONS_BOTH):
        default_action = IgnoreAction([])

        action_same_file = actions.get("same_file", default_action)

        action_different_type = actions.get("different_type", default_action)
        action_different_type_left = actions.get("different_type_left", default_action)
        action_different_type_right = actions.get("different_type_right", default_action)

        action_directory = actions.get("directory", default_action)
        action_directory_left = actions.get("directory_left", default_action)
        action_directory_right = actions.get("directory_right", default_action)

        action_different_size = actions.get("different_size", default_action)
        action_different_size_left = actions.get("different_size_left", default_action)
        action_different_size_right = actions.get("different_size_right", default_action)

        action_empty = actions.get("empty", default_action)
        action_empty_left = actions.get("empty_left", default_action)
        action_empty_right = actions.get("empty_right", default_action)

        action_missing_hash = actions.get("missing_hash", default_action)
        action_missing_hash_left = actions.get("missing_hash_left", default_action)
        action_missing_hash_right = actions.get("missing_hash_right", default_action)

        action_same_size_no_hash = actions.get("same_size_no_hash", default_action)
        action_same_size_no_hash_left = actions.get("same_size_no_hash_left", default_action)
        action_same_size_no_hash_right = actions.get("same_size_no_hash_right", default_action)

        action_same_hash = actions.get("same_hash", default_action)
        action_same_hash_left = actions.get("same_hash_left", default_action)
        action_same_hash_right = actions.get("same_hash_right", default_action)

        action_different_hash = actions.get("different_hash", default_action)
        action_different_hash_left = actions.get("different_hash_left", default_action)
        action_different_hash_right = actions.get("different_hash_right", default_action)

        for key in sorted(aset & bset):
            if ignore and by in ("relpath", "abspath") and ignore.match(fspath(key)):
                continue

            aprops = a[key]
            bprops = b[key]

            if aprops.abspath and bprops.abspath and os.path.samefile(aprops.abspath, bprops.abspath):
                action_same_file.do_both("same_file", key, aprops, bprops)
                continue

            if aprops.isdir != bprops.isdir:
                action_different_type.do_both("different_type", key, aprops, bprops)
                action_different_type_left.do_single("different_type", key, aprops)
                action_different_type_right.do_single("different_type", key, bprops)
                continue

            if aprops.isdir:
                action_directory.do_both("directory", key, aprops, bprops)
                action_directory_left.do_single("directory", key, aprops)
                action_directory_right.do_single("directory", key, bprops)
                continue

            if aprops.size != bprops.size:
                action_different_size.do_both("different_size", key, aprops, bprops)
                action_different_size_left.do_single("different_size", key, aprops)
                action_different_size_right.do_single("different_size", key, bprops)
                continue

            if aprops.size == 0 and bprops.size == 0:
                action_empty.do_both("empty", key, aprops, bprops)
                action_empty_left.do_single("empty", key, aprops)
                action_empty_right.do_single("empty", key, bprops)
                continue

            if (not aprops.hash or not bprops.hash) and hasher is not None:  # missing hashes should be computed
                if (aprops.hash or aprops.abspath) and (bprops.hash or bprops.abspath):
                    if not aprops.hash:
                        aprops.hash = hasher.get(Path(aprops.abspath))  # type: ignore [arg-type]
                    if not bprops.hash:
                        bprops.hash = hasher.get(Path(bprops.abspath))  # type: ignore [arg-type]
                else:  # missing hashes cannot be computed
                    action_missing_hash.do_both("missing_hash", key, aprops, bprops)
                    action_missing_hash_left.do_single("missing_hash", key, aprops)
                    action_missing_hash_right.do_single("missing_hash", key, bprops)

            if not aprops.hash or not bprops.hash:
                action_same_size_no_hash.do_both("same_size_no_hash", key, aprops, bprops)
                action_same_size_no_hash_left.do_single("same_size_no_hash", key, aprops)
                action_same_size_no_hash_right.do_single("same_size_no_hash", key, bprops)
                continue

            if aprops.hash == bprops.hash:
                action_same_hash.do_both("same_hash", key, aprops, bprops)
                action_same_hash_left.do_single("same_hash", key, aprops)
                action_same_hash_right.do_single("same_hash", key, bprops)
            else:
                action_different_hash.do_both("different_hash", key, aprops, bprops)
                action_different_hash_left.do_single("different_hash", key, aprops)
                action_different_hash_right.do_single("different_hash", key, bprops)


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


def make_rel(iter_func: Callable[[str], Iterator[FileProperties]]) -> Callable[[str, str], Iterator[FileProperties]]:
    def inner(path: str, rel_to: str) -> Iterator[FileProperties]:
        for prop in iter_func(path):
            assert prop.relpath is not None
            prop.relpath = os.path.relpath(prop.relpath, rel_to)
            yield prop

    return inner


def with_hash(it: Iterable[FileProperties], hasher: Optional[Hasher] = None) -> Iterator[FileProperties]:
    for prop in it:
        assert prop.abspath is not None
        if prop.hash is None and hasher is not None:
            prop.hash = hasher.get(Path(prop.abspath))
        yield prop


ProgressType = TypeVar("ProgressType")


def track_files(
    sequence: Iterable[ProgressType],
    description: str = "Working...",
) -> Iterable[ProgressType]:
    progress = Progress(
        TextColumn("{task.completed} files collected from {task.description}"),
        TaskProgressColumn(show_speed=True),
        TimeElapsedColumn(),
    )
    with progress:
        yield from progress.track(sequence, description=description)


ACTIONS = {
    "ignore": IgnoreAction,
    "print": PrintAction,
    "copy": CopyAction,
    "trash": TrashAction,
}


def get_all_actions():
    all_actions = ["left_only", "right_only", "same_file"] + ACTIONS_BOTH
    for action in ACTIONS_BOTH:
        all_actions.append(f"{action}_left")
        all_actions.append(f"{action}_right")
    return all_actions


PRESETS = {
    "print-different": {
        "left_only": "print",
        "right_only": "print",
        "different_type": "print",
        "different_size": "print",
        "different_hash": "print",
    }
}


def action_names_from_args(args) -> Dict[str, str]:
    all_actions = get_all_actions()
    action_names: Dict[str, str] = {}

    if args.preset is None:
        for a in all_actions:
            action_names[a] = args.default_action
    else:
        preset = PRESETS[args.preset]
        for a in all_actions:
            action_names[a] = preset.get(a) or "ignore"

    for a in all_actions:
        action = getattr(args, f"{a}_action")
        if action is not None:
            action_names[a] = action

    return action_names


class GetActions:
    def __init__(self, action_names: Dict[str, str], **kwargs) -> None:
        action_types = defaultdict(list)
        for k, v in action_names.items():
            action_types[v].append(k)
        self._action_objects: Dict[str, BaseAction] = {}
        for k, v in action_types.items():
            if k is None:
                cls = IgnoreAction
            else:
                cls = ACTIONS[k]
            self._action_objects[k] = cls(v, **kwargs)

        self.actions = {k: self._action_objects[v] for k, v in action_names.items()}

    def __enter__(self) -> Dict[str, BaseAction]:
        return self.actions

    def __exit__(self, *args):
        for obj in self._action_objects.values():
            obj.close()


if __name__ == "__main__":
    from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser

    from platformdirs import user_data_dir

    types = {
        "archive": iter_archive,
        "torrent": iter_torrent,
        "torrents": iter_torrents,
        "qb-fastresume": iter_fastresumes,
        "dir": iter_dir,
        "dir-with-archives": partial(iter_dir, recurse_archives=True, hash_from_meta=False),
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
        formatter_class=ArgumentDefaultsHelpFormatter,
        description="Compare contents of two different file trees (called left and right here) to each other. Various different sources, like local directories, archives and torrent files are supported.",
    )
    parser.add_argument("--left-paths", nargs="+", type=path_or_url, required=True, help="Left group of input paths")
    parser.add_argument("--right-paths", nargs="+", type=path_or_url, required=True, help="Right group of input paths")
    parser.add_argument(
        "--left-rel",
        nargs="+",
        default=None,
        help="Top level directory of left path everything will be relative to. Must match the provided paths.",
    )
    parser.add_argument(
        "--right-rel",
        nargs="+",
        default=None,
        help="Top level directory of right path everything will be relative to. Must match the provided paths.",
    )
    parser.add_argument("--left-type", choices=types.keys(), default="auto", help="Type of left content")
    parser.add_argument(
        "--right-type",
        choices=types.keys(),
        default="auto",
        help="Type of right content",
    )
    parser.add_argument(
        "--by",
        choices={"relpath", "hash", "abspath", "size"},
        default="relpath",
        help="File property to use as the group key",
    )
    parser.add_argument(
        "--hashfunc",
        choices=("sha1", "md5", "crc32"),
        default=None,
        help="Hash function used to compare file contents. If not provided, the content will not be compared. If provided, it's first tried to read the hash value from the file meta info, if not available it will be computed from the file contents. If the contents is not available also, a warning is printed.",
    )
    parser.add_argument(
        "--no-dirs",
        action="store_true",
        help="Ignore missing directories. Use for input types which don't have explicit directories, like torrent files.",
    )
    parser.add_argument("--default-action", default="print")
    parser.add_argument("--preset", choices=PRESETS.keys())
    parser.add_argument("--out-path", type=Path, help="Output file for print action, default is stdout")
    parser.add_argument("--copy-dst", type=Path, help="Base dir for copy action")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--hashcache",
        type=Path,
        default=DEFAULT_HASHCACHE,
        help="Path to sqlite database file to load/store cached hashes",
    )
    action_group = parser.add_argument_group(title="actions", description="Finegrained actions to take")
    for action in get_all_actions():
        action = action.replace("_", "-")
        action_group.add_argument(f"--{action}-action", choices=ACTIONS.keys())
    args = parser.parse_args()

    if (args.left_rel or args.right_rel) and args.by != "relpath":
        parser.error("--left-rel or --right-rel can only be used with --by relpath")

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

    if args.by in ("relpath", "abspath"):
        apply: Optional[Callable] = Path  # necessary for case-insensitive path compares under Windows
    else:
        apply = None

    if args.by == "relpath":
        left_it = map(make_rel(iter_func_a), map(fspath, args.left_paths), args.left_rel)
        right_it = map(make_rel(iter_func_b), map(fspath, args.right_paths), args.right_rel)
    else:
        left_it = map(iter_func_a, map(fspath, args.left_paths))
        right_it = map(iter_func_b, map(fspath, args.right_paths))

    left_it = chain.from_iterable(left_it)
    right_it = chain.from_iterable(right_it)

    if args.hashfunc:
        hasher: Optional[Hasher] = Hasher(args.hashfunc, args.hashcache)
    else:
        hasher = None
        logging.warning(
            "No hash function is given, so matching paths will not be verified further and grouping by hash is only supported when hash meta info is available."
        )

    if args.by == "hash":
        try:
            left_it = with_hash(left_it, hasher)
        except AttributeError:
            logging.critical("%s doesn't supporting grouping by %s", funcname(iter_func_b), args.by)
            sys.exit(1)
        try:
            right_it = with_hash(right_it, hasher)
        except AttributeError:
            logging.critical("%s doesn't supporting grouping by %s", funcname(iter_func_b), args.by)
            sys.exit(1)

    if args.verbose:
        left_it = track_files(left_it, description="left")
        right_it = track_files(right_it, description="right")

    try:
        a = iterable_to_dict_by_key(args.by, left_it, apply)
    except AttributeError:
        logging.critical("%s doesn't supporting grouping by %s", funcname(iter_func_a), args.by)
        sys.exit(1)

    try:
        b = iterable_to_dict_by_key(args.by, right_it, apply)
    except AttributeError:
        logging.critical("%s doesn't supporting grouping by %s", funcname(iter_func_b), args.by)
        sys.exit(1)

    action_names = action_names_from_args(args)
    with GetActions(action_names, out_path=args.out_path, copy_dst=args.copy_dst) as actions:
        compare(a, b, hasher, actions, by=args.by, ignore=re.compile(r"^.*\.pyc$"))
