from __future__ import generator_stop

import csv
import os.path
from datetime import datetime, timezone
from functools import partial
from gzip import GzipFile
from io import TextIOWrapper, open
from itertools import chain
from typing import TYPE_CHECKING
from xml.etree.ElementTree import iterparse

import requests
from genutility.args import existing_path
from genutility.datetime import datetime_from_utc_timestamp, datetime_from_utc_timestamp_ns
from genutility.file import _check_arguments
from genutility.filesystem import FileProperties, scandir_rec
from genutility.hash import crc32_hash_file
from genutility.torrent import iter_torrent

if TYPE_CHECKING:
	from pathlib import Path
	from typing import Callable, Dict, Iterable, Iterator, Optional

""" current limitations: All paths on one side, need to be of the same type.
	Auto type detection will only use the first entry for type determination.

"""

class OpenFileOrUrl:

	def __init__(self, path, mode="rt", encoding="utf-8"):
		self.encoding = encoding

		encoding = _check_arguments(mode, encoding)

		if path.startswith(("http://", "https://")):

			if "w" in mode:
				raise ValueError("Cannot write mode for URLs")

			r = requests.get(path, stream=True)
			r.raise_for_status()
			if r.headers["content-encoding"] == "gzip":
				fileobj = GzipFile(fileobj=r.raw)
			else:
				fileobj = r.raw

			if "t" in mode:
				self.f = TextIOWrapper(fileobj, encoding=self.encoding)
			else:
				self.f = fileobj
		else:
			self.f = open(path, mode, encoding=self.encoding)

	def __enter__(self):
		return self.f

	def __exit__(self, *args):
		self.close()

	def close(self):
		self.f.close()

def iter_archiveorg_xml(path, hashfunc="sha1", dirs=None):
	# type: (str, str, Optional[bool]) -> Iterator[FileProperties]

	assert hashfunc in ("sha1", "md5", "crc32")

	skip_formats = {"Metadata", "Archive BitTorrent", "Item Tile"}

	with OpenFileOrUrl(path) as fr:
		for event, element in iterparse(fr, ["end"]):
			if element.tag != "file":
				continue
			if element.get("source") != "original":
				continue
			if element.find("format").text in skip_formats:
				continue

			relpath = element.get("name")
			size = int(element.find("size").text)
			modtime = datetime_from_utc_timestamp(int(element.find("mtime").text))
			hash = element.find(hashfunc).text

			yield FileProperties(relpath, size, False, modtime=modtime, hash=hash)

def iter_gamedat_xml(path, hashfunc="sha1", dirs=None):
	# type: (str, str, Optional[bool]) -> Iterator[FileProperties]

	assert hashfunc in ("sha1", "md5", "crc32")

	hashfunc = {
		"crc32": "crc",
	}.get(hashfunc, hashfunc)

	with open(path, "rt") as fr:
		for event, element in iterparse(fr, ["end"]):
			if element.tag != "game":
				continue

			rom = element.find("rom")
			relpath = rom.get("name")
			size = rom.get("size")
			hash = rom.get(hashfunc)
			yield FileProperties(relpath, size, False, hash=hash)

def iter_zip(archivefile, topleveldir=None, hashfunc="crc32", assume_utc=False, dirs=None):
	# type: (str, Optional[str], str, bool, Optional[bool]) -> Iterator[FileProperties]

	""" If `topleveldir` is given, returned file paths will be relativ to this directory within the archive.

		If `assume_utc` is False (the default), it is assumed that local time is stored
		in the zip file. Otherwise it's assumed to be UTC.
	"""

	from zipfile import ZipFile

	assert hashfunc in {"crc32"}

	with ZipFile(archivefile, "r") as af:
		for f in af.infolist():

			if topleveldir:
				relpath = os.path.relpath(f.filename, topleveldir)
			else:
				relpath = f.filename

			year, month, day, hour, minute, second = f.date_time
			if assume_utc:
				# interpret as utc
				modtime = datetime(year, month, day, hour, minute, second, tz=timezone.utc)
			else:
				# interpret as local time
				modtime = datetime(year, month, day, hour, minute, second).astimezone(timezone.utc)

			yield FileProperties(relpath, f.file_size, f.is_dir(), modtime=modtime, hash=f.CRC)

def iter_rar(archivefile, topleveldir=None, hashfunc="crc32", dirs=None):
	# type: (str, Optional[str], str, Optional[bool]) -> Iterator[FileProperties]

	from rarfile import RarFile

	assert hashfunc in {"crc32"}

	with RarFile(archivefile, "r") as af:
		for f in af.infolist():

			if topleveldir:
				relpath = os.path.relpath(f.filename, topleveldir)
			else:
				relpath = f.filename

			yield FileProperties(relpath, f.file_size, f.is_dir(), modtime=f.mtime, hash=f.CRC)

def iter_archive(archivefile, topleveldir=None, hashfunc="crc32"):
	if archivefile.endswith(".zip"):
		return iter_zip(archivefile, topleveldir, hashfunc)
	elif archivefile.endswith(".rar"):
		return iter_rar(archivefile, topleveldir, hashfunc)
	else:
		assert False, "Unsupported archive format"

def iter_dir(path, extra=True, hashfunc=None, dirs=True):
	# type: (str, bool, Optional[str], bool) -> Iterator[FileProperties]

	""" Returns correct device id and file inode for py > 3.5 on windows if `extras=True` """

	for entry in scandir_rec(path, files=True, dirs=dirs, relative=True):

		if extra:
			stat = os.stat(entry.path)
		else:
			stat = entry.stat()

		modtime = datetime_from_utc_timestamp_ns(stat.st_mtime_ns)

		yield FileProperties(entry.relpath.replace("\\", "/"), stat.st_size, entry.is_dir(), entry.path, (stat.st_dev, stat.st_ino), modtime)

def iter_syncthing(path, extra=True, versions=".stversions", hashfunc=None):
	""" skips syncthing versions folder """

	for entry in scandir_rec(path, files=True, dirs=True, relative=True, allow_skip=True):
		if entry.name == versions and entry.is_dir():
			entry.follow = False
			continue

		if extra:
			stat = os.stat(entry.path)
		else:
			stat = entry.stat()

		modtime = datetime_from_utc_timestamp_ns(stat.st_mtime_ns)

		yield FileProperties(entry.relpath, stat.st_size, entry.is_dir(), entry.path, (stat.st_dev, stat.st_ino), modtime)

def files_to_csv(files, csvpath):
	# type: (Iterable[FileProperties], str) -> None

	with open(csvpath, "w", newline="", encoding="utf-8") as csvfile:
		csvwriter = csv.writer(csvfile)
		csvwriter.writerow(FileProperties.keys())
		for props in files:
			csvwriter.writerow(props.values())

def compare(a, b, left=True, right=True, both=True, ignore=None):
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

def iterable_to_dict_by_key(by, it):
	# type: (Iterable[FileProperties], ) -> Dict[str, tuple]

	# todo: check if there are duplicated keys and warn about them

	return {getattr(props, by): props for props in it}

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
	import re
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
	a = iterable_to_dict_by_key(args.by, chain.from_iterable(map(iter_func_a, args.left_paths)))
	b = iterable_to_dict_by_key(args.by, chain.from_iterable(map(iter_func_b, args.right_paths)))

	compare(a, b, ignore=re.compile(r"^.*\.pyc$"))
