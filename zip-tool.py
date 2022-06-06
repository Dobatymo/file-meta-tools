from zipfile import ZIP_DEFLATED, ZipFile

from genutility.atomic import TransactionalCreateFile
from rich.progress import Progress

"""
7z a -tzip -stl -mx9 -mtc- filename.7z.zip "directory"
"""


def zip_has_extra(path):
    with ZipFile(path, "r") as af:
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
                with ZipFile(path, "r") as a_in, ZipFile(
                    f_tmp, "x", ZIP_DEFLATED, allowZip64=True, compresslevel=9
                ) as a_out:
                    infolist = a_in.infolist()
                    task2 = progress.add_task(str(relpath), total=len(infolist))
                    for info in infolist:
                        if info.is_dir():
                            info.date_time = (1980, 1, 1, 0, 0, 0)
                        info.extra = b""
                        assert info.comment == b""
                        with a_in.open(info, "r") as f_in, a_out.open(info, "w") as f_out:
                            f_out.write(f_in.read())
                        progress.advance(task2)
                    progress.remove_task(task2)

            progress.advance(task1)


def slots_to_dict(obj):
    return {slot: getattr(obj, slot) for slot in obj.__slots__}


def compare_zip_meta(path_a, path_b):
    with ZipFile(path_a, "r") as zip_a, ZipFile(path_b, "r") as zip_b:
        for info_a, info_b in zip(zip_a.infolist(), zip_b.infolist()):
            d_a = slots_to_dict(info_a)
            d_b = slots_to_dict(info_b)
            if d_a != d_b:
                print(d_a, d_b)
                return False
    return True


if __name__ == "__main__":
    from argparse import ArgumentParser

    from genutility.args import is_dir, is_file

    parser = ArgumentParser()
    parser.add_argument("action", choices=("has-extra", "rewrite-without-extra", "compare-meta"))
    parser.add_argument("--path", type=is_dir)
    parser.add_argument("--path-left", type=is_file)
    parser.add_argument("--path-right", type=is_file)
    args = parser.parse_args()

    if args.action == "has-extra":
        find_zip_with_extra(args.path)
    elif args.action == "rewrite-without-extra":
        rewrite_zip_without_extra(args.path)
    elif args.action == "compare-meta":
        compare_zip_meta(args.path_left, args.path_right)
