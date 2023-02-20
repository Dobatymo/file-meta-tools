import logging
import os
from pathlib import Path
from typing import List

import enzyme  # pip install enzyme
from genutility.filesystem import scandir_ext


def disabled_tracks(mkv: enzyme.MKV) -> List[enzyme.Track]:
    return [
        track
        for tracks in (mkv.video_tracks, mkv.audio_tracks, mkv.subtitle_tracks)
        for track in tracks
        if not track.enabled
    ]


def scandir_error_log_warning(entry: os.DirEntry, exception) -> None:
    logging.warning("<%s> OSError: %s", entry.path, exception)


def main(basepath: Path, recursive: bool = True) -> None:
    logging.getLogger("enzyme.parsers.ebml.core").setLevel(logging.CRITICAL)

    for path in scandir_ext(
        basepath,
        extensions={".mkv", ".mka", ".mks", ".mk3d", ".webm"},
        rec=recursive,
        errorfunc=scandir_error_log_warning,
    ):
        strpath = os.fspath(path)
        try:
            with open(strpath, "rb") as f:
                mkv = enzyme.MKV(f)
        except enzyme.MalformedMKVError as e:
            logging.warning("<%s> MalformedMKVError: %s", strpath, e)
            continue
        except KeyError as e:
            logging.error("<%s> KeyError: %s", strpath, e)
            continue
        except Exception as e:
            logging.exception("<%s> Exception: %s", strpath, e)
            continue

        tracks = disabled_tracks(mkv)
        if tracks:
            print(strpath, tracks)


if __name__ == "__main__":
    from argparse import ArgumentParser

    from genutility.args import is_dir

    parser = ArgumentParser()
    parser.add_argument("path", type=is_dir, help="Directory to scan for mkv files with disabled tracks")
    parser.add_argument("-r", "--recursive", action="store_true", help="Scan recursively")
    args = parser.parse_args()

    main(args.path, args.recursive)
