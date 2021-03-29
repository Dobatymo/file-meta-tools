import logging
import platform
import sqlite3
from builtins import print as _print
from os import fspath, stat
from pathlib import Path
from string import ascii_uppercase
from typing import TYPE_CHECKING

from genutility.filesystem import scandir_rec
from genutility.logging import IsoDatetimeFormatter
from genutility.sql import CursorContext
from genutility.sqlite import batch_executer
from genutility.win.file import is_open_for_write

if TYPE_CHECKING:
	from os import PathLike
	from typing import Dict, Iterator, List, Optional, Tuple

	from genutility.typing import Connection

	FileID = Tuple[int, int]
	FilesDict = Dict[FileID, Tuple[str, str, int, int]]
	FilesTuple = Tuple[int, int, str, str, int, int]

logger = logging.getLogger(__name__)

def get_all_drives_windows():
	return [drive for driveletter in ascii_uppercase if (drive := Path(driveletter + ":\\")).is_dir()]

ALL_DRIVES = get_all_drives_windows()
DEFAULT_DB_PATH = f"{platform.node()}-catalog.db"

def print(*msg, end="\x1b[0K\n", **kwargs):
	_print(*msg, end=end, **kwargs)

def is_signed_int_64(num):
	return -2**63 <= num <= 2**63-1

def unsigned_to_signed_int_64(num):
	return num - 2**63

def signed_to_unsigned_int_64(num):
	return num + 2**63

def read_dir(path):
	# type: (str, ) -> FilesDict

	root = path.replace("\\", "/")

	def scandir_error_log(entry, exception):
		logger.warning("Error in %s: %s", entry.path, exception)

	def it():
		for entry in scandir_rec(path, files=True, dirs=False, relative=True, errorfunc=scandir_error_log):

			try:
				stats = stat(entry.path)
			except (PermissionError, FileNotFoundError) as e:
				logger.warning("Ignoring '%s' because of: %s", entry.path, e)
				continue
			except OSError as e:
				logger.error("Ignoring '%s' because of: %s", entry.path, e)
				continue

			relpath = entry.relpath.replace("\\", "/")
			if stats.st_dev != 0 and stats.st_ino != 0:
				# On windows st_dev and st_ino is unsigned 64 bit int, but sqlite only supports signed 64 bit ints.
				# I don't know about linux
				device = unsigned_to_signed_int_64(stats.st_dev)
				inode = unsigned_to_signed_int_64(stats.st_ino)
				assert is_signed_int_64(inode) and is_signed_int_64(device) and is_signed_int_64(stats.st_size) and is_signed_int_64(stats.st_mtime_ns), (stats.st_dev, stats.st_ino, stats.st_size, stats.st_mtime_ns)
				yield (device, inode), (root, relpath, stats.st_size, stats.st_mtime_ns)
			else:
				logger.warning("Ignoring '%s' because of invalid id (device ID=%s, file inode=%s)", entry.path, stats.st_dev, stats.st_ino)

	return dict(it())

class FilesDB:

	def __init__(self, path, case_insensitive=None):
		# type: (str, Optional[bool]) -> None

		self.conn = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)
		self.conn.isolation_level = None

		if case_insensitive is None:
			self.case_insensitive = platform.system() in ("Windows", "Darwin")
		else:
			self.case_insensitive = case_insensitive

	def init(self):
		# type: () -> None

		create_table_query = """CREATE TABLE IF NOT EXISTS files (
			id INTEGER PRIMARY KEY,
			begin_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			end_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			device INTEGER NOT NULL,
			inode INTEGER NOT NULL,
			root TEXT NOT NULL,
			path TEXT NOT NULL,
			filesize INTEGER NOT NULL,
			utcmtime INTEGER NOT NULL,
			deleted INTEGER NOT NULL DEFAULT 0
		);"""

		idx_files = "CREATE INDEX IF NOT EXISTS idx_files_id ON files (device, inode);"
		if self.case_insensitive:
			idx_root = "CREATE INDEX IF NOT EXISTS idx_root ON files (root COLLATE NOCASE);"
		else:
			idx_root = "CREATE INDEX IF NOT EXISTS idx_root ON files (root);"
		idx_deleted = "CREATE INDEX IF NOT EXISTS idx_deleted ON files (deleted);"

		create_index_queries = [idx_files, idx_root, idx_deleted]

		with CursorContext(self.conn) as cur:
			cur.execute(create_table_query)
			for query in create_index_queries:
				cur.execute(query)

	def get_connection(self):
		# type: () -> Connection

		return self.conn

	def get(self, deleted=False):
		# type: (bool, ) -> Iterator[FilesTuple]

		query = "SELECT device, inode, root, path, filesize, utcmtime FROM files WHERE deleted=?"

		with CursorContext(self.conn) as cursor:
			yield from cursor.execute(query, (int(deleted), ))

	def get_by_root(self, root, deleted=False):
		# type: (PathLike, bool) -> Iterator[FilesTuple]

		root = fspath(root).replace("\\", "/")
		if self.case_insensitive:
			query = "SELECT device, inode, root, path, filesize, utcmtime FROM files WHERE deleted=? AND root=? COLLATE NOCASE"
		else:
			query = "SELECT device, inode, root, path, filesize, utcmtime FROM files WHERE deleted=? AND root=?"

		with CursorContext(self.conn) as cursor:
			yield from cursor.execute(query, (int(deleted), root))

	def read_database(self, path, deleted=False):
		# type: (PathLike, bool) -> FilesDict

		return {(device, inode): (root, path, filesize, utcmtime)
			for device, inode, root, path, filesize, utcmtime in self.get_by_root(path, deleted)
		}

def updated_nodes(fs, db):
	# type: (FilesDict, FilesDict) -> Tuple[List[FileID], List[FileID]]

	equal = []
	updated = []
	for id in fs.keys() & db.keys():
		fs_info = fs[id]
		db_info = db[id]
		if fs_info == db_info:
			equal.append(id)
		else:
			root, path, filesize, utcmtime = fs_info
			fullpath = root + path

			try:
				in_use = is_open_for_write(fullpath)
			except (PermissionError, FileNotFoundError) as e:
				logger.warning("Error: %s", e)
				continue
			except OSError:
				logger.exception("Cannot check if file is in use")
				continue

			if not in_use:
				updated.append(id)
			else:
				logger.info("Ignoring in-use file: %s", fullpath)

	return equal, updated

def compare(conn, fs, db):
	# type: (Connection, str, FilesDict, FilesDict) -> None

	"""	Algorithm:

		Assumes a file ID which is unique at any moment in time, but can change or be reused at other times.

		- Find all new file IDs:
			Find all files IDs which are on disk and compare to the ones which are not marked as deleted in db.
			This might find IDs which existed before but have been deleted since and will ignore files
			were a new file reused the id in-between runs.
		- Delete old files:
			Compare all files on disk with db and mark non-existing ones as deleted in db.
		- Find modifications:
			For all files which are neither new nor deleted, compare the properties to see if the files have changed.
			If the file hasn't changed, then it doesn't matter if the ID was reused and the end_date can be updated.
			To only update the most recent occurrence of the ID only update files which are not marked as deleted.
			If the file has changed, it will be marked as delete and inserted as new file regardless of a reused ID.
	"""

	fs_nodes = fs.keys()
	db_nodes = db.keys()

	def fsdata_for_node(id):
		device, inode = id
		root, path, filesize, utcmtime = fs[id]
		return device, inode, root, path, filesize, utcmtime

	with CursorContext(conn) as cursor:

		# insert nodes not in database yet
		new_nodes = map(fsdata_for_node, fs_nodes - db_nodes)
		new_nodes_query = "INSERT INTO files (device, inode, root, path, filesize, utcmtime) VALUES (?, ?, ?, ?, ?, ?)"
		entries = batch_executer(cursor, new_nodes_query, new_nodes)
		print(f"Added {entries} new files")

		# set nodes to deleted which don't exist in filesystem anymore
		deleted_nodes_query = "UPDATE files SET deleted=1 WHERE device=? AND inode=? AND deleted=0"
		entries = batch_executer(cursor, deleted_nodes_query, db_nodes - fs_nodes)
		print(f"Deleted {entries} files")

		unmodified, modified = updated_nodes(fs, db)

		# update last change date of unmodified files
		unmodified_nodes_query = "UPDATE files SET end_date=CURRENT_TIMESTAMP WHERE device=? AND inode=? AND deleted=0"
		entries = batch_executer(cursor, unmodified_nodes_query, unmodified)
		print(f"Found {entries} unmodified files")

		# for files which where modified
		# - set them to deleted for current properties
		# - add them with new properties
		batch_executer(cursor, deleted_nodes_query, modified)
		modified_nodes = map(fsdata_for_node, modified)
		entries = batch_executer(cursor, new_nodes_query, modified_nodes)
		print(f"Found {entries} modified files")

if __name__ == "__main__":
	from argparse import ArgumentParser

	import colorama
	from genutility.args import is_dir

	parser = ArgumentParser()
	parser.add_argument("--drives", type=is_dir, default=ALL_DRIVES, nargs="+")
	parser.add_argument("--db-path", type=str, default=DEFAULT_DB_PATH)
	parser.add_argument("-v", "--verbose", action="store_true")
	args = parser.parse_args()

	fmt = "%(asctime)s\t%(levelname)s\t%(filename)s:%(lineno)d\t%(funcName)s: %(message)s"
	handler = logging.FileHandler(args.db_path + ".log", encoding="utf-8")
	formatter = IsoDatetimeFormatter(fmt, sep=" ", timespec="milliseconds", aslocal=True)
	handler.setFormatter(formatter)
	logger.addHandler(handler)

	if args.verbose:
		logger.setLevel(logging.INFO)
	else:
		logger.setLevel(logging.WARNING)

	colorama.init()

	db = FilesDB(args.db_path)
	db.init()
	conn = db.get_connection()

	for root in args.drives:
		print(f"Cataloging {root}")
		dbdict = db.read_database(root)
		fsdict = read_dir(fspath(root))
		print(f"Read {len(fsdict)} directory and {len(dbdict)} database entries")
		compare(conn, fsdict, dbdict)
