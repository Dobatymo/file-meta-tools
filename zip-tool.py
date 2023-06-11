import zipfile
from pathlib import Path

from genutility.atomic import TransactionalCreateFile
from genutility.file import copyfilelike
from rich.progress import Progress

from filemetatools import zip

"""
7z a -tzip -stl -mx9 -mtc- filename.7z.zip "directory"
"""


def find_zip_with_extra(path: Path) -> None:
    for path in path.rglob("*.zip"):
        has_extras = zip.zip_has_extra(path)
        print(has_extras, path)


def rewrite_zip_without_extra(basepath: Path) -> None:
    """Rewrite all zip files in directory without any extra metainfo.
    This can be used to create more easily reproducible zip files or remove unwanted pii.
    """

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


def compare_zip_meta(path_a, path_b):
    with zipfile.ZipFile(path_a, "r") as zip_a, zipfile.ZipFile(path_b, "r") as zip_b:
        for info_a, info_b in zip(zip_a.infolist(), zip_b.infolist()):
            d_a = zip.slots_to_dict(info_a)
            d_b = zip.slots_to_dict(info_b)
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
        assert args.path.is_file()
        zip.copy_zip(args.path, args.path.with_suffix(".new.zip"))
