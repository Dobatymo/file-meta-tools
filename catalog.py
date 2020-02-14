import logging
from datetime import datetime, timezone
import sqlite3

from future.utils import viewkeys

from ferutility.sql import TransactionCursor, CursorContext, sqlite_batch_executer
from genutility.filesystem import scandir_rec
from genutility.iter import iter_equal
from genutility.win.file import is_open_for_write

from compare import iter_dir

"""scan:
	- query by inode:
		if deleted flag set:
			insert as new row with current start and enddate (and optional hash)
		if deleted flag not set::
			compare filename, size, moddate and optionally hash
			if all match:
				update updated enddate
			if not all match:
				insert new row with current start and enddate (and optional hash)
query all inodes with deleted flag not set:
	set deleted flag for all which dont exist anymore"""

create_table_query = """CREATE TABLE IF NOT EXISTS files (
	id INTEGER PRIMARY KEY,
	begin_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	end_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	device INTEGER NOT NULL,
	inode INTEGER NOT NULL,
	path INTEGER NOT NULL,
	filesize INTEGER NOT NULL,
	utcmtime TIMESTAMP NOT NULL,
	deleted INTEGER NOT NULL DEFAULT 0
);"""

create_index_query = "CREATE INDEX IF NOT EXISTS files_id ON files (device, inode);"

def read_dir(path):
	def it():
		for props in iter_dir(path):
			dev, ino = props.id
			if dev != 0 and ino != 0:
				yield props.id, (props.relpath, props.size, props.modtime) 
			else:
				logging.warning("Ignoring '%s' because of zero %s (device ID, file inode)", props.relpath, props.id)

	return dict(it())

def read_database(conn):
	query = "SELECT device, inode, path, filesize, utcmtime FROM files WHERE deleted=0"
	#for row in conn.cursor().execute(query):
	#	print(row)
	return {(device, inode): (path, filesize, utcmtime) for device, inode, path, filesize, utcmtime in conn.cursor().execute(query)}

def compare(conn, fs, db):
	fs_nodes = viewkeys(fs)
	db_nodes = viewkeys(db)

	def fsdata_for_node(id):
		device, inode = id
		path, filesize, utcmtime = fs[id]
		return device, inode, path, filesize, utcmtime

	# insert nodes not in database yet
	new_nodes = map(fsdata_for_node, fs_nodes - db_nodes)
	new_nodes_query = "INSERT INTO files (device, inode, path, filesize, utcmtime) VALUES (?, ?, ?, ?, ?)"
	entries = sqlite_batch_executer(conn.cursor(), new_nodes_query, new_nodes)
	print("Added {} new files".format(entries))

	# det nodes to deleted which don't exist in filesystem anymore
	deleted_nodes_query = "UPDATE files SET deleted=1 WHERE device=? AND inode=? AND deleted=0"
	entries = sqlite_batch_executer(conn.cursor(), deleted_nodes_query, db_nodes - fs_nodes)
	print("Deleted {} files".format(entries))

	def updated_nodes():
		equal = []
		updated = []
		for id in fs_nodes & db_nodes:
			fs_info = fs[id]
			db_info = db[id]
			if iter_equal(fs_info, db_info):
				equal.append(id)
			else:
				path, filesize, utcmtime = fs_info
				if not is_open_for_write(path):
					updated.append(id)
				else:
					logging.warning("Ignoring open file: %s", path)

		return equal, updated

	unmodified, modified = updated_nodes()

	# update last change date of unmodified files
	unmodified_nodes_query = "UPDATE files SET end_date=CURRENT_TIMESTAMP WHERE device=? AND inode=? AND deleted=0"
	entries = sqlite_batch_executer(conn.cursor(), unmodified_nodes_query, unmodified)
	print("Found {} unmodified files".format(entries))

	# for files which where modified
	# - set them to deleted for current properties
	# - add them with new properties
	modified_nodes_query = "UPDATE files SET deleted=1 WHERE device=? AND inode=? AND deleted=0"
	sqlite_batch_executer(conn.cursor(), modified_nodes_query, modified)
	modified_nodes = map(fsdata_for_node, modified)
	entries = sqlite_batch_executer(conn.cursor(), new_nodes_query, modified_nodes)
	print("Found {} modified files".format(entries))

if __name__ == "__main__":
	import platform
	localname = platform.node()

	conn = sqlite3.connect("{}-catalog.db".format(localname), detect_types=sqlite3.PARSE_DECLTYPES)
	conn.isolation_level = None

	with CursorContext(conn) as cur:
		cur.execute(create_table_query)
		cur.execute(create_index_query)

	fs = read_dir("E:\\")
	db = read_database(conn)
	print("Read {} directory and {} database entries".format(len(fs), len(db)))
	compare(conn, fs, db)
