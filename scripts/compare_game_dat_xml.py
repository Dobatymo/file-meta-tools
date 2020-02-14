from future.utils import viewkeys
from genutility.file import read_file

from ..compare import iter_archiveorg_xml, iter_gamedat_xml

def strs_in_str(strings, s):
	for string in strings:
		if string in s:
			return True
	return False

def split_gamedat(it):
	asia_strs = ["(Japan)", "(Korea)"]
	usa_strs = ["(USA)"]
	europe_strs = ["(Europe)", "(Australia)", "(Europe, Australia)", "(Germany)", "(France)", "(UK)", "(Spain)", "(Italy)", "(Sweden)", "(Netherlands)"]

	asia, usa, europe = {}, {}, {}

	for props in it:
		if strs_in_str(asia_strs, props.relpath):
			asia[props.hash] = props.relpath
		elif strs_in_str(usa_strs, props.relpath):
			usa[props.hash] = props.relpath
		elif strs_in_str(europe_strs, props.relpath):
			europe[props.hash] = props.relpath
		else:
			print(props.relpath)

archive_asia = "../data/AsiaGamecubeCollectionByGhostware_files.xml"
archive_usa = "../data/GamecubeCollectionByGhostware_files.xml"
archive_europe = "../data/EuropeanGamecubeCollectionByGhostware_files.xml"
game_dat = "../data/Nintendo - GameCube - Datfile (1894) (2020-02-04 03-16-06).dat"

archive_asia_set = set(props.hash for props in iter_archiveorg_xml(archive_asia))
archive_usa_set = set(props.hash for props in iter_archiveorg_xml(archive_usa))
archive_europe_set = set(props.hash for props in iter_archiveorg_xml(archive_europe))
asia, usa, europe = split_gamedat(iter_gamedat_xml(game_dat))

def do(country_str, archive_set, dat_dict):

	print()
	print("---"+country_str+"---")
	print()

	print("only in archive_xml")
	only_archive = archive_set - viewkeys(dat_dict)
	print(len(only_archive), only_archive)

	print("only in game_dat")
	only_dat =  viewkeys(dat_dict) - archive_set
	for sha1 in only_dat:
		print(sha1, dat_dict[sha1])

do("ASIA", archive_asia_set, asia)
do("USA", archive_usa_set, usa)
do("EUROPA", archive_europe_set, europe)
