import random
import string
from pathlib import Path


def create_file(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as fw:
        start = b"_START_"
        end = b"_END_"
        k = max(0, size - len(start) - len(end))
        b = string.ascii_letters + string.digits
        data = start + "".join(random.choices(b, k=k)).encode("ascii") + end  # nosec
        fw.write(data[:size])


def create_files(basepath: Path) -> None:
    create_file(basepath / "file0", 0)
    create_file(basepath / "file999", 999)
    create_file(basepath / "folder" / "file10", 10)


if __name__ == "__main__":
    create_files(Path("zipfile"))
