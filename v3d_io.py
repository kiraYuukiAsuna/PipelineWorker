import sys
import os
import struct
import v3d_io
import numpy as np


"""
Load and save v3draw and raw stack files that are supported by V3D
by Lei Qu
20200330
"""


def load_v3d_raw_img_file(filename):
    im = {}
    try:
        f_obj = open(filename, 'rb')
    except FileNotFoundError:
        print("ERROR: Failed in reading [" + filename + "], Exit.")
        f_obj.close()
        return im
    else:
        # read image header - formatkey(24bytes)
        len_formatkey = len('raw_image_stack_by_hpeng')
        formatkey = f_obj.read(len_formatkey)
        formatkey = struct.unpack(str(len_formatkey) + 's', formatkey)
        if formatkey[0] != b'raw_image_stack_by_hpeng':
            print("ERROR: File unrecognized (not raw, v3draw) or corrupted.")
            f_obj.close()
            return im

        # read image header - endianCode(1byte)
        endiancode = f_obj.read(1)
        endiancode = struct.unpack('c', endiancode)  # 'c' = char
        endiancode = endiancode[0]
        if endiancode != b'B' and endiancode != b'L':
            print("ERROR: Only supports big- or little- endian,"
                  " but not other format. Check your data endian.")
            f_obj.close()
            return im

        # read image header - datatype(2bytes)
        datatype = f_obj.read(2)
        if endiancode == b'L':
            datatype = struct.unpack('<h', datatype)  # 'h' = short
        else:
            datatype = struct.unpack('>h', datatype)  # 'h' = short
        datatype = datatype[0]
        if datatype < 1 or datatype > 4:
            print("ERROR: Unrecognized data type code [%d]. "
                  "The file type is incorrect or this code is not supported." % (datatype))
            f_obj.close()
            return im

        # read image header - size(4*4bytes)
        size = f_obj.read(4 * 4)
        if endiancode == b'L':
            size = struct.unpack('<4l', size)  # 'l' = long
        else:
            size = struct.unpack('>4l', size)  # 'l' = long
        # print(size)

        # read image data
        npixels = size[0] * size[1] * size[2] * size[3]
        im_data = f_obj.read()
        if datatype == 1:
            im_data = np.frombuffer(im_data, np.uint8)
        elif datatype == 2:
            im_data = np.frombuffer(im_data, np.uint16)
        else:
            im_data = np.frombuffer(im_data, np.float32)
        if len(im_data) != npixels:
            print("ERROR: Read image data size != image size. Check your data.")
            f_obj.close()
            return im

        im_data = im_data.reshape((size[3], size[2], size[1], size[0]))
        # print(im_data.shape)
        im_data = np.moveaxis(im_data, 0, -1)
        # print(im_data.shape)
        im_data = np.moveaxis(im_data, 0, -2)
        # print(im_data.shape)
    f_obj.close()

    im['endian'] = endiancode
    im['datatype'] = datatype
    im['size'] = im_data.shape
    im['data'] = im_data

    return im


def save_v3d_raw_img_file(im, filename):
    if filename[-4:] != '.raw' and filename[-7:] != '.v3draw':
        print("ERROR: Unsupportted file format. return!")

    try:
        f_obj = open(filename, 'wb')
    except FileNotFoundError:
        print("ERROR: Failed in writing [" + filename + "], Exit.")
        f_obj.close()
        return im
    else:
        # write image header - formatkey(24bytes)
        formatkey = b'raw_image_stack_by_hpeng'
        formatkey = struct.pack('<24s', formatkey)
        f_obj.write(formatkey)

        # write image header - endianCode(1byte)
        endiancode = b'L'
        endiancode = struct.pack('<s', endiancode)
        f_obj.write(endiancode)

        # write image header - datatype(2bytes)
        datatype = im['datatype']
        datatype = struct.pack('<h', datatype)  # 'h' = short
        f_obj.write(datatype)

        # write image header - size(4*4bytes)
        size = im['size']
        size = struct.pack('<4l', size[1], size[0],
                           size[2], size[3])  # 'l' = long
        f_obj.write(size)

        # write image data
        im_data = im['data']
        im_data = np.moveaxis(im_data, -2, 0)
        im_data = np.moveaxis(im_data, -1, 0)
        im_data = im_data.tobytes()
        f_obj.write(im_data)
    f_obj.close()


def readmarker(filepath):
    markerpos = []
    with open(filepath, 'r') as f:
        while True:
            l = f.readline()
            if l == "":
                break
            if l[0] == "#":
                continue
            l = l.split(',')
            markerpos.append(
                [np.float64(l[0]), np.float64(l[1]), np.float64(l[2])])
    return np.array(markerpos, dtype=np.float64)


def write_v3d_header(filename, size, datatype=2):
    """
    写入v3d文件头

    Args:
        filename: 输出文件路径
        size: 图像尺寸 (X, Y, Z, C)
        datatype: 数据类型 (1=uint8, 2=uint16, 4=float32)
    """
    try:
        f_obj = open(filename, 'wb')
    except:
        print(f"ERROR: Failed to create file [{filename}]")
        return False

    try:
        # write image header - formatkey(24bytes)
        formatkey = b'raw_image_stack_by_hpeng'
        formatkey = struct.pack('<24s', formatkey)
        f_obj.write(formatkey)

        # write image header - endianCode(1byte)
        endiancode = b'L'
        endiancode = struct.pack('<s', endiancode)
        f_obj.write(endiancode)

        # write image header - datatype(2bytes)
        datatype = struct.pack('<h', datatype)  # 'h' = short
        f_obj.write(datatype)

        # write image header - size(4*4bytes)
        # 输入size是 (X, Y, Z, C)，但v3d格式写入顺序是 (Y, X, Z, C)
        size_packed = struct.pack('<4l', size[1], size[0], size[2], size[3])  # 'l' = long
        f_obj.write(size_packed)

        f_obj.close()
        return True
    except Exception as e:
        print(f"ERROR: Failed to write header to [{filename}]: {e}")
        f_obj.close()
        return False


def append_v3d_chunk(filename, chunk_data):
    """
    向v3d文件追加数据块

    Args:
        filename: v3d文件路径
        chunk_data: 数据块，应该是 (C, Z, Y, X) 格式的numpy数组
    """
    try:
        f_obj = open(filename, 'ab')  # 追加模式
    except:
        print(f"ERROR: Failed to open file [{filename}] for appending")
        return False

    try:
        # 确保数据顺序为 (C, Z, Y, X)
        if chunk_data.ndim != 4:
            print(f"ERROR: chunk_data should be 4D (C,Z,Y,X), got {chunk_data.ndim}D")
            f_obj.close()
            return False

        # 写入数据块
        # v3d格式存储顺序为 (C, Z, Y, X)，直接写入
        chunk_bytes = chunk_data.tobytes()
        f_obj.write(chunk_bytes)

        f_obj.close()
        return True
    except Exception as e:
        print(f"ERROR: Failed to append chunk to [{filename}]: {e}")
        f_obj.close()
        return False


def load_v3d_chunk(filename, z_start, z_end):
    """
    从v3d文件加载指定Z范围的数据块

    Args:
        filename: v3d文件路径
        z_start: Z起始位置
        z_end: Z结束位置

    Returns:
        chunk_data: (Y, X, Z, C) 格式的numpy数组
    """
    try:
        f_obj = open(filename, 'rb')
    except FileNotFoundError:
        print(f"ERROR: Failed in reading [{filename}]")
        return None

    try:
        # 读取文件头
        len_formatkey = len('raw_image_stack_by_hpeng')
        formatkey = f_obj.read(len_formatkey)
        formatkey = struct.unpack(str(len_formatkey) + 's', formatkey)
        if formatkey[0] != b'raw_image_stack_by_hpeng':
            print("ERROR: File unrecognized (not raw, v3draw) or corrupted.")
            f_obj.close()
            return None

        # 读取端序
        endiancode = f_obj.read(1)
        endiancode = struct.unpack('c', endiancode)[0]

        # 读取数据类型
        datatype = f_obj.read(2)
        if endiancode == b'L':
            datatype = struct.unpack('<h', datatype)[0]
        else:
            datatype = struct.unpack('>h', datatype)[0]

        # 读取尺寸
        size = f_obj.read(16)
        if endiancode == b'L':
            size = struct.unpack('<4l', size)
        else:
            size = struct.unpack('>4l', size)

        # size存储顺序为 (Y, X, Z, C)，转换为 (X, Y, Z, C)
        X, Y, Z, C = size[1], size[0], size[2], size[3]

        # 确定数据类型
        if datatype == 1:
            dtype = np.uint8
            bytes_per_pixel = 1
        elif datatype == 2:
            dtype = np.uint16
            bytes_per_pixel = 2
        else:
            dtype = np.float32
            bytes_per_pixel = 4

        # 计算Z范围
        z_start = max(0, z_start)
        z_end = min(Z, z_end)
        chunk_z = z_end - z_start

        if chunk_z <= 0:
            f_obj.close()
            return None

        # 计算数据偏移
        pixels_per_z_per_channel = X * Y
        bytes_per_z_per_channel = pixels_per_z_per_channel * bytes_per_pixel

        # 跳过前面的Z层
        skip_bytes = z_start * bytes_per_z_per_channel * C
        f_obj.seek(f_obj.tell() + skip_bytes)

        # 读取目标Z范围的数据
        read_bytes = chunk_z * bytes_per_z_per_channel * C
        data_bytes = f_obj.read(read_bytes)

        if len(data_bytes) != read_bytes:
            print(f"ERROR: Expected {read_bytes} bytes, got {len(data_bytes)}")
            f_obj.close()
            return None

        # 重构数据
        chunk_data = np.frombuffer(data_bytes, dtype=dtype)
        chunk_data = chunk_data.reshape((C, chunk_z, Y, X))

        # 转换为 (Y, X, Z, C) 格式
        chunk_data = np.moveaxis(chunk_data, [0, 1, 2, 3], [3, 2, 0, 1])  # (C,Z,Y,X) -> (Y,X,Z,C)

        f_obj.close()
        return chunk_data

    except Exception as e:
        print(f"ERROR: Failed to load chunk from [{filename}]: {e}")
        f_obj.close()
        return None
