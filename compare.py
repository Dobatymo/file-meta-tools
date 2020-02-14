from builtins import map
from future.utils import viewkeys

import csv, os.path, zipfile
from io import open
from xml.etree.ElementTree import iterparse
from datetime import datetime
from itertools import chain

from genutility.filesystem import scandir_rec, uncabspath, FileProperties
from genutility.hash import crc32_hash_file
from genutility.torrent import iter_torrent
from genutility.args import existing_path

""" current limitations: All paths on one side, need to be of the same type.
	Auto type detection will only use the first entry for type determination.


"""

def iter_archiveorg_xml(path, hashfunc="sha1"):
	assert hashfunc in ("sha1", "md5", "crc32")

	with open(path, "rt") as fr:
		for event, element in iterparse(fr, ["end"]):
			if element.tag != "file":
				continue
			if element.get("source") != "original":
				continue
			if element.find("format").text in ("Metadata", "Archive BitTorrent"):
				continue

			relpath = element.get("name")
			size = element.find("size").text
			modtime = element.find("mtime").text
			hash = element.find(hashfunc).text

			yield FileProperties(relpath, size, False, modtime=modtime, hash=hash)

def iter_gamedat_xml(path, hashfunc="sha1"):
	assert hashfunc in ("sha1", "md5", "crc")

	with open(path, "rt") as fr:
		for event, element in iterparse(fr, ["end"]):
			if element.tag != "game":
				continue

			rom = element.find("rom")
			relpath = rom.get("name")
			size = rom.get("size")
			hash = rom.get(hashfunc)
			yield FileProperties(relpath, size, False, hash=hash)

def _iter_archive(ArchiveFile, archivepath, topleveldir=None):
	with ArchiveFile(archivepath, "r") as af:
		for f in af.infolist():
			if topleveldir:
				yield FileProperties(os.path.relpath(f.filename, topleveldir), f.file_size, f.isdir(), modtime=f.mtime, hash=f.CRC)
			else:
				yield FileProperties(f.filename, f.file_size, f.isdir(), modtime=f.mtime, hash=f.CRC)

def iter_rar(archivefile, topleveldir=None):
	import rarfile

	return _iter_archive(rarfile.RarFile, archivefile, topleveldir)

def iter_zip(archivefile, topleveldir=None):
	return _iter_archive(zipfile.ZipFile, archivefile, topleveldir)

def iter_archive(archivefile, topleveldir=None):
	if archivefile.endswith(".zip"):
		return iter_zip(archivefile, topleveldir)
	elif archivefile.endswith(".rar"):
		return iter_rar(archivefile, topleveldir)
	else:
		assert False, "Unsupported archive format"

def iter_dir(path, extra=True):
	# types: (str, bool) -> Iterator[FileProperties]

	""" Returns correct device id and file inode for py > 3.5 on windows if `extras=True` """

	for entry in scandir_rec(path, files=True, dirs=True, relative=True):
		abspath = os.path.join(path, entry.path)

		if extra:
			stat = os.stat(abspath)
		else:
			stat = entry.stat()

		modtime = datetime.utcfromtimestamp(stat.st_mtime)

		yield FileProperties(entry.path, stat.st_size, entry.is_dir(), abspath, (stat.st_dev, stat.st_ino), modtime)

def iter_syncthing(path, versions=".stversions"):
	""" skips syncthing versions folder """

	for entry in scandir_rec(path, files=True, dirs=True, relative=True):
		if entry.name == versions and entry.is_dir():
			entry.follow = False
			continue
		stat = entry.stat()
		modtime = datetime.utcfromtimestamp(stat.st_mtime)
		abspath = os.path.join(path, entry.path)
		yield FileProperties(entry.path, stat.st_size, entry.is_dir(), abspath, (stat.st_dev, stat.st_ino), modtime)

def files_to_csv(files, csvpath):
	# types: (Iterable[FileProperties], str) -> None

	with open(csvpath, "w", newline="", encoding="utf-8") as csvfile:
		csvwriter = csv.writer(csvfile)
		for props in files:
			csvwriter.writerow(props)

def compare(a, b, left=True, right=True, both=True, ignore=None):
	aset = viewkeys(a)
	bset = viewkeys(b)

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
					print("bo:", "size different", key)
				elif aprops.size == 0 and bprops.size == 0:
					pass
				else: # same size
					if (aprops.hash or aprops.abspath) and (bprops.hash or bprops.abspath):
						if not aprops.hash:
							aprops.hash = int(crc32_hash_file(aprops.abspath), 16)
						if not bprops.hash:
							bprops.hash = int(crc32_hash_file(bprops.abspath), 16)
						if aprops.hash != bprops.hash:
							print("bo:", "hash different", key, aprops.hash, bprops.hash)
						# else: pass # same files
					else:
						print("bo:", "no hash or abspath for same size files", key)

def iterable_to_dict_by_key(by, it):
	# types: (Iterable[FileProperties], ) -> Dict[str, tuple]

	# todo: check if there are duplicated keys and warn about them

	return {getattr(props, by): props for props in it}

def find_type(path):
	# types: (Path, ) -> Callable

	if path.is_dir():
		return iter_dir
	elif path.is_file():
		if path.suffix == ".torrent":
			return iter_torrent
		elif path.suffix in {"rar", "zip"}:
			return iter_archive
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

	parser = ArgumentParser(description="Compare contents of two different file trees to each other. Various different sources, like local directories, archives and torrent files are supported.")
	parser.add_argument("--left-paths", nargs="+", type=existing_path, required=True)
	parser.add_argument("--right-paths", nargs="+", type=existing_path, required=True)
	parser.add_argument("--left-rel", nargs="+", default=None, help="Top level directory of left path everything will be relative to")
	parser.add_argument("--right-rel", nargs="+", default=None, help="Top level directory of right path everything will be relative to")
	parser.add_argument("--left-type", choices=types.keys(), default="auto", help="Type of left content")
	parser.add_argument("--right-type", choices=types.keys(), default="auto", help="Type of right content")
	parser.add_argument("--by", choices={"relpath", "hash"}, default="relpath")
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
		iter_func_a = types[args.left_type]

	if args.right_type == "auto":
		iter_func_b = find_type(args.right_paths[0])
	else:
		iter_func_b = types[args.right_type]

	print("Using input functions:", iter_func_a.__name__, iter_func_b.__name__)

	#a = iterable_to_dict_by_key(iter_func_a(args.archive, args.topleveldir))
	a = iterable_to_dict_by_key(args.by, chain.from_iterable(map(iter_func_a, args.left_paths)))
	b = iterable_to_dict_by_key(args.by, chain.from_iterable(map(iter_func_b, args.right_paths)))

	compare(a, b, ignore=re.compile(r"^.*\.pyc$"))
