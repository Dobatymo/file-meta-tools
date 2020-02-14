# compare

## some examples

### compare rom dat file to collection on archive.org

`py compare.py --left-paths "./data/Nintendo - GameCube - Datfile (1894) (2020-02-04 03-16-06).dat" --right-paths "./data/AsiaGamecubeCollectionByGhostware_files.xml" "./data/EuropeanGamecubeCollectionByGhostware_files.xml" "./data/GamecubeCollectionByGhostware_files.xml" --by hash`

### compare files from multiple torrents to one directory

`py compare.py --left-paths "OverClocked ReMix v1" --right-paths "OverClocked ReMix v1 (0001-1000).torrent" "OverClocked ReMix v1 (1001-1900).torrent"`
`py compare.py --left-paths "OverClocked ReMix v2" --right-paths "OverClocked ReMix v2 (0001-1000).torrent" "OverClocked ReMix v2 (1001-2000).torrent" "OverClocked ReMix v2 (2001-2500).torrent"`
