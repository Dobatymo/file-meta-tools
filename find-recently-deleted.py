from filemeta.listreaders import iter_dir, iter_syncthing
from filemeta.utils import iterable_to_dict_by_key
from genutility.filesystem import FileProperties

from compare import Hasher, compare


def clean_syncthing_versioning(props: FileProperties) -> FileProperties:
    if not props.isdir:
        assert props.relpath is not None
        base, dateandext = props.relpath.rsplit("~", 1)
        try:
            date, ext = dateandext.rsplit(".", 1)
            props.relpath = base + "." + ext
        except ValueError:
            props.relpath = base

    return props


if __name__ == "__main__":
    import os.path
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--versions", default=".stversions")
    args = parser.parse_args()

    left = iter_syncthing(args.path, args.versions)
    right = map(clean_syncthing_versioning, iter_dir(os.path.join(args.path, args.versions)))

    a = iterable_to_dict_by_key("relpath", left)
    b = iterable_to_dict_by_key("relpath", right)

    hasher = Hasher()
    compare(a, b, hasher, left=None, both=None)
