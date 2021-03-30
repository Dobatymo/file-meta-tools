from __future__ import generator_stop

import logging
from os import fspath
from typing import TYPE_CHECKING, List, Tuple

from genutility.logging import IsoDatetimeFormatter
from genutility.sql import CursorContext
from genutility.sqlite import batch_executer
from genutility.win.file import is_open_for_write

from filemetatools.fsreaders import FilesDB, read_dir
from filemetatools.utils import DEFAULT_DB_PATH, get_all_drives_windows

if TYPE_CHECKING:
	from genutility.typing import Connection

	from filemetatools.fsreaders import FileID, FilesDict

logger = logging.getLogger(__name__)

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
	# type: (Connection, FilesDict, FilesDict) -> None

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

	ALL_DRIVES = get_all_drives_windows()

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
