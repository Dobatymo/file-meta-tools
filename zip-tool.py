import zipfile
from pathlib import Path
from typing import Optional

from genutility.atomic import TransactionalCreateFile
from genutility.file import copyfilelike
from rich.progress import Progress

"""
7z a -tzip -stl -mx9 -mtc- filename.7z.zip "directory"
"""


def zip_has_extra(path):
    with zipfile.ZipFile(path, "r") as af:
        for info in af.infolist():
            if info.extra:
                return True
    return False


def find_zip_with_extra(path):
    for path in path.rglob("*.zip"):
        has_extras = zip_has_extra(path)
        print(has_extras, path)


def rewrite_zip_without_extra(basepath):
    with Progress(transient=True) as progress:
        task1 = progress.add_task("Processing...", total=None)

        for path in basepath.rglob("*.zip"):
            relpath = path.relative_to(basepath)
            with TransactionalCreateFile(path, "xb", handle_archives=False) as f_tmp:
                with zipfile.ZipFile(path, "r") as a_in, zipfile.ZipFile(
                    f_tmp, "x", zipfile.ZIP_DEFLATED, allowZip64=True, compresslevel=9
                ) as a_out:
                    infolist = a_in.infolist()
                    task2 = progress.add_task(str(relpath), total=len(infolist))
                    for info in infolist:
                        if info.is_dir():
                            info.date_time = (1980, 1, 1, 0, 0, 0)
                        info.extra = b""
                        assert info.comment == b""
                        with a_in.open(info, "r") as f_in, a_out.open(info, "w") as f_out:
                            copyfilelike(f_in, f_out)
                        progress.advance(task2)
                    progress.remove_task(task2)

            progress.advance(task1)


def slots_to_dict(obj):
    return {slot: getattr(obj, slot) for slot in obj.__slots__}


def compare_zip_meta(path_a, path_b):
    with zipfile.ZipFile(path_a, "r") as zip_a, zipfile.ZipFile(path_b, "r") as zip_b:
        for info_a, info_b in zip(zip_a.infolist(), zip_b.infolist()):
            d_a = slots_to_dict(info_a)
            d_b = slots_to_dict(info_b)
            if d_a != d_b:
                print(d_a, d_b)
                return False
    return True


class DummyCompress:
    def __init__(
        self,
        level: int,
        method: int,
        wbits: int,
        memLevel: Optional[int] = None,
        strategy: Optional[int] = None,
        zdict: Optional[bytes] = None,
    ):
        pass

    def compress(self, data: bytes) -> bytes:
        return data

    def copy(self):
        return RuntimeError("Not implemented")

    def flush(self, mode: Optional[int] = None) -> bytes:
        return b""


class DummyDecompress:
    unconsumed_tail = b""
    eof = False

    def __init__(self, wbits: int, zdict: Optional[bytes] = None):
        pass

    def copy(self):
        return RuntimeError("Not implemented")

    def decompress(self, data: bytes, max_length: int = 0) -> bytes:
        unconsumed_tail = data[max_length:]  # noqa
        return data[:max_length]

    def flush(self, length: int = 16384) -> bytes:
        return b""


class dummy_zlib:
    compressobj = DummyCompress
    decompressobj = DummyDecompress
    Z_DEFAULT_COMPRESSION = 0
    DEFLATED = 0


def copy_zip(path_in, path_out):
    from copy import copy

    zipfile.zlib = dummy_zlib

    with zipfile.ZipFile(path_in, "r") as a_in, zipfile.ZipFile(
        path_out, "w", zipfile.ZIP_DEFLATED, allowZip64=True, compresslevel=9
    ) as a_out:
        infolist = a_in.infolist()
        for info in infolist:
            _info = copy(info)
            assert slots_to_dict(_info) == slots_to_dict(info)
            with a_in.open(info, "r") as f_in, a_out.open(info, "w") as f_out:
                # disable CRC verification on reading
                f_in._expected_crc = None
                # restore old meta info

                copyfilelike(f_in, f_out)
                # print("input", slots_to_dict(_info))
                # print("output", slots_to_dict(f_out._zinfo))
                # these values are overwritten in write file close methods
                # so overwrite them here first
                f_out._crc = _info.CRC
                f_out._file_size = _info.file_size
                if f_out._zinfo.header_offset == _info.header_offset:
                    f_out._zinfo = _info
                else:
                    # copy info bits
                    f_out._zinfo.flag_bits = _info.flag_bits
                    f_out._zinfo.internal_attr = _info.internal_attr
                    f_out._zinfo.external_attr = _info.external_attr
                    f_out._zinfo.compress_size = _info.compress_size


if __name__ == "__main__":
    from argparse import ArgumentParser

    from genutility.args import is_file

    parser = ArgumentParser()
    parser.add_argument("action", choices=("has-extra", "rewrite-without-extra", "compare-meta", "copy-zip"))
    parser.add_argument("--path", type=Path)
    parser.add_argument("--path-left", type=is_file)
    parser.add_argument("--path-right", type=is_file)
    args = parser.parse_args()

    if args.action == "has-extra":
        assert args.path.is_dir()
        find_zip_with_extra(args.path)
    elif args.action == "rewrite-without-extra":
        assert args.path.is_dir()
        rewrite_zip_without_extra(args.path)
    elif args.action == "compare-meta":
        assert args.path_left.is_file()
        assert args.path_right.is_file()
        compare_zip_meta(args.path_left, args.path_right)
    elif args.action == "copy-zip":
        assert args.path.is_file()
        copy_zip(args.path, args.path.with_suffix(".new.zip"))
