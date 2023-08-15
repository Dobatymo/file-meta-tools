import zipfile
from pathlib import Path

from genutility.atomic import TransactionalCreateFile
from genutility.file import copyfilelike
from genutility.rich import Progress, get_double_format_columns
from rich.progress import Progress as RichProgress

from filemetatools import zip as ziptool

"""
7z a -tzip -stl -mx9 -mtc- filename.7z.zip "directory"
"""


def find_zip_with_extra(path: Path) -> None:
    for path in path.rglob("*.zip"):
        has_extras = ziptool.zip_has_extra(path)
        print(has_extras, path)


def rewrite_zip_without_extra(basepath: Path) -> None:
    """Rewrite all zip files in directory without any extra metainfo.
    This can be used to create more easily reproducible zip files or remove unwanted pii.
    """

    with RichProgress(*get_double_format_columns()) as progress:
        p = Progress(progress)
        for path in p.track(basepath.rglob("*.zip"), description="Processed {task.completed} zip files"):
            relpath = path.relative_to(basepath)
            with TransactionalCreateFile(path, "xb", handle_archives=False) as f_tmp:
                with zipfile.ZipFile(path, "r") as a_in, zipfile.ZipFile(
                    f_tmp, "x", zipfile.ZIP_DEFLATED, allowZip64=True, compresslevel=9
                ) as a_out:
                    infolist = a_in.infolist()
                    for info in p.track(infolist, description=str(relpath)):
                        if info.is_dir():
                            info.date_time = (1980, 1, 1, 0, 0, 0)
                        info.extra = b""
                        assert info.comment == b""
                        with a_in.open(info, "r") as f_in, a_out.open(info, "w") as f_out:
                            copyfilelike(f_in, f_out)


def compare_zip_meta(path_a, path_b) -> bool:
    with zipfile.ZipFile(path_a, "r") as zip_a, zipfile.ZipFile(path_b, "r") as zip_b:
        for info_a, info_b in zip(zip_a.infolist(), zip_b.infolist()):
            d_a = ziptool.slots_to_dict(info_a)
            d_b = ziptool.slots_to_dict(info_b)
            if d_a != d_b:
                print(d_a, d_b)
                return False
    return True


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
        if args.path is None:
            parser.error("--path is required")
        if not args.path.is_file():
            parser.error("--path must be a file")
        ziptool.copy_zip(args.path, args.path.with_suffix(".new.zip"))
