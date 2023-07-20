import zipfile
from typing import Optional

from genutility.file import copyfilelike


def slots_to_dict(obj):
    return {slot: getattr(obj, slot) for slot in obj.__slots__}


def zip_has_extra(path) -> bool:
    with zipfile.ZipFile(path, "r") as af:
        for info in af.infolist():
            if info.extra:
                return True
    return False


class DummyCompress:
    def __init__(
        self,
        level: int,
        method: int,
        wbits: int,
        memLevel: Optional[int] = None,
        strategy: Optional[int] = None,
        zdict: Optional[bytes] = None,
    ):
        pass

    def compress(self, data: bytes) -> bytes:
        return data

    def copy(self):
        return RuntimeError("Not implemented")

    def flush(self, mode: Optional[int] = None) -> bytes:
        return b""


class DummyDecompress:
    def __init__(self, wbits: int, zdict: Optional[bytes] = None):
        self.unconsumed_tail = b""
        self.eof = False

    def copy(self):
        return RuntimeError("Not implemented")

    def decompress(self, data: bytes, max_length: int = 0) -> bytes:
        if max_length == 0:
            if self.unconsumed_tail:
                out = self.unconsumed_tail + data
                self.unconsumed_tail = b""
                return out
            else:
                return data

        if self.unconsumed_tail or len(data) > max_length:
            tail_len = len(self.unconsumed_tail)
            data_len = max(0, max_length - tail_len)
            out = self.unconsumed_tail[:max_length] + data[:data_len]
            self.unconsumed_tail = self.unconsumed_tail[max_length:] + data[data_len:]
            return out

        return data

    def flush(self, length: int = 16384) -> bytes:
        out = self.unconsumed_tail[:length]
        self.unconsumed_tail = self.unconsumed_tail[length:]
        return out


class dummy_zlib:
    compressobj = DummyCompress
    decompressobj = DummyDecompress
    Z_DEFAULT_COMPRESSION = 0
    DEFLATED = 0


def copy_zip(path_in, path_out, extras: bool = True, comments: bool = True) -> None:
    """Copy zip from path_in to path_out without re-compressing the files,
    simply rewrite the zip file entries.
    """

    from copy import copy

    _zlib = zipfile.zlib  # type: ignore[attr-defined]
    zipfile.zlib = dummy_zlib  # type: ignore[attr-defined]

    try:
        with zipfile.ZipFile(path_in, "r") as a_in, zipfile.ZipFile(
            path_out, "w", zipfile.ZIP_DEFLATED, allowZip64=True, compresslevel=9
        ) as a_out:
            infolist = a_in.infolist()
            for info in infolist:
                _info = copy(info)
                assert slots_to_dict(_info) == slots_to_dict(info)

                if not extras:
                    _info.extra = b""
                if not comments:
                    _info.comment = b""

                real_file_size = info.file_size
                real_crc = info.CRC

                info.file_size = info.compress_size

                with a_in.open(info, "r") as f_in, a_out.open(_info, "w") as f_out:
                    # disable CRC verification on reading
                    f_in._expected_crc = None
                    # restore old meta info

                    copyfilelike(f_in, f_out)
                    i_in = slots_to_dict(_info)
                    i_out = slots_to_dict(f_out._zinfo)
                    assert i_in == i_out, (i_in, i_out)
                    # these values are overwritten in write file close methods
                    # so overwrite them here first
                    f_out._crc = real_crc
                    f_out._file_size = real_file_size
                    if f_out._zinfo.header_offset == _info.header_offset:
                        f_out._zinfo = _info
                    else:
                        # copy info bits
                        f_out._zinfo.flag_bits = _info.flag_bits
                        f_out._zinfo.internal_attr = _info.internal_attr
                        f_out._zinfo.external_attr = _info.external_attr
                        f_out._zinfo.compress_size = _info.compress_size
    finally:
        zipfile.zlib = _zlib  # type: ignore[attr-defined]
