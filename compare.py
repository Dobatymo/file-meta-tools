from __future__ import generator_stop

import csv
import re
from functools import partial
from itertools import chain
from os import fspath
from typing import TYPE_CHECKING, Callable, Dict, Iterable

from genutility.args import existing_path
from genutility.filesystem import FileProperties
from genutility.hash import crc32_hash_file
from genutility.torrent import iter_torrent

from filemeta.listreaders import (iter_archive, iter_archiveorg_xml, iter_dir, iter_gamedat_xml, iter_rar,
                                       iter_syncthing, iter_zip)
from filemeta.utils import iterable_to_dict_by_key

if TYPE_CHECKING:
	from pathlib import Path

	from filemetatools.utils import HashableLessThan

""" current limitations: All paths on one side, need to be of the same type.
	Auto type detection will only use the first entry for type determination.

"""


def files_to_csv(files, csvpath):
	# type: (Iterable[FileProperties], str) -> None

	with open(csvpath, "w", newline="", encoding="utf-8") as csvfile:
		csvwriter = csv.writer(csvfile)
		csvwriter.writerow(FileProperties.keys())
		for props in files:
			csvwriter.writerow(props.values())

def compare(a, b, left=True, right=True, both=True, ignore=None):
	# type: (Dict[HashableLessThan, FileProperties], Dict[HashableLessThan, FileProperties], bool, bool, bool, re.Pattern) -> None

	aset = a.keys()
	bset = b.keys()

	#retleft = None
	#retright = None

	# note: the key is usually the `relpath` or the `hash`

	if left:
		print("In left only")
		for key in sorted(aset - bset):
			if ignore and ignore.match(key):
				continue
			print("lo:", key, a[key].relpath)

	if right:
		print("In right only")
		for key in sorted(bset - aset):
			if ignore and ignore.match(key):
				continue
			print("ro:", key, b[key].relpath)

	if both:
		print("On both, but different")
		for key in sorted(aset & bset):
			if ignore and ignore.match(key):
				continue

			aprops = a[key]
			bprops = b[key]

			if aprops.isdir != bprops.isdir:
				print("bo:", "one is dir, one is file", key)
			if not aprops.isdir:
				if aprops.size != bprops.size:
					print("bo:", "size different", key, aprops.size, bprops.size)
				elif aprops.size == 0 and bprops.size == 0:
					pass
				else: # same size
					if (aprops.hash or aprops.abspath) and (bprops.hash or bprops.abspath):
						if not aprops.hash:
							#aprops.hash = int(crc32_hash_file(aprops.abspath), 16)
							aprops.hash = crc32_hash_file(aprops.abspath)
						if not bprops.hash:
							#bprops.hash = int(crc32_hash_file(bprops.abspath), 16)
							bprops.hash = crc32_hash_file(bprops.abspath)
						if aprops.hash != bprops.hash:
							print("bo:", "hash different", key, aprops.hash, bprops.hash)
						# else: pass # same files
					else:
						print("bo:", "no hash or abspath for same size files", key)

def find_type(path):
	# type: (Path, ) -> Callable

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

if __name__ == "__main__":
	from argparse import ArgumentParser

	types = {
		"archive": iter_archive,
		"torrent": iter_torrent,
		"dir": iter_dir,
		"archiveorg_xml": iter_archiveorg_xml,
		"gamedat_xml": iter_gamedat_xml,
		"syncthing": iter_syncthing,
	}

	def path_or_url(path):
		if path.startswith(("http://", "https://")):
			return path
		return existing_path(path)

	parser = ArgumentParser(description="Compare contents of two different file trees to each other. Various different sources, like local directories, archives and torrent files are supported.")
	parser.add_argument("--left-paths", nargs="+", type=path_or_url, required=True)
	parser.add_argument("--right-paths", nargs="+", type=path_or_url, required=True)
	parser.add_argument("--left-rel", nargs="+", default=None, help="Top level directory of left path everything will be relative to")
	parser.add_argument("--right-rel", nargs="+", default=None, help="Top level directory of right path everything will be relative to")
	parser.add_argument("--left-type", choices=types.keys(), default="auto", help="Type of left content")
	parser.add_argument("--right-type", choices=types.keys(), default="auto", help="Type of right content")
	parser.add_argument("--by", choices={"relpath", "hash"}, default="relpath")
	parser.add_argument("--hashfunc", choices=("sha1", "md5", "crc32"), default=None)
	parser.add_argument("--no-dirs", action="store_true")
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
		iter_func_a = find_type(args.left_paths[0])
	else:
		if args.hashfunc:
			iter_func_a = partial(types[args.left_type], hashfunc=args.hashfunc, dirs=not args.no_dirs)
		else:
			iter_func_a = partial(types[args.left_type], dirs=not args.no_dirs)

		iter_func_a.__name__ = types[args.left_type].__name__

	if args.right_type == "auto":
		iter_func_b = find_type(args.right_paths[0])
	else:
		if args.hashfunc:
			iter_func_b = partial(types[args.right_type], hashfunc=args.hashfunc, dirs=not args.no_dirs)
		else:
			iter_func_b = partial(types[args.right_type], dirs=not args.no_dirs)

		iter_func_b.__name__ = types[args.right_type].__name__

	print("Using input functions:", iter_func_a.__name__, iter_func_b.__name__)

	#a = iterable_to_dict_by_key(iter_func_a(args.archive, args.topleveldir))
	a = iterable_to_dict_by_key(args.by, chain.from_iterable(map(iter_func_a, map(fspath, args.left_paths))))
	b = iterable_to_dict_by_key(args.by, chain.from_iterable(map(iter_func_b, map(fspath, args.right_paths))))

	compare(a, b, ignore=re.compile(r"^.*\.pyc$"))
