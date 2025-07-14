import gc
import subprocess
import sys
import numpy as np
import h5py
import os
from functools import reduce
import psutil
import tqdm

try:
    from v3d_io import write_v3d_header, append_v3d_chunk
    USE_CHUNKED_WRITE = True
except ImportError:
    USE_CHUNKED_WRITE = False
    print("Warning: v3d_io chunked write functions not available, using standard write")

def get_memory_usage():
    """获取当前内存使用情况(MB)"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def monitor_memory():
    """监控内存使用情况"""
    mem = psutil.virtual_memory()
    return {
        'used_gb': mem.used / (1024**3),
        'available_gb': mem.available / (1024**3),
        'percent': mem.percent
    }

def calculate_dimensions(filepath, cur_level, rows, cols):
    """预计算维度信息"""
    with h5py.File(filepath, 'r') as f:
        # 获取第一个块的形状来确定维度
        sample_shape = f['0'][str(cur_level)]['0'][()].shape
        print(f"Sample shape: {sample_shape}")
        
        # 如果是4D，我们只保留前三个维度
        if len(sample_shape) == 4:
            sample_shape = sample_shape[:-1]  # 移除最后一个维度
        
        # 计算每行高度和每列宽度
        row_heights = np.zeros(rows, dtype=np.int32)
        col_widths = np.zeros(cols, dtype=np.int32)
        
        # 顺序处理每个块以避免一次性加载所有块的形状
        for i in range(rows * cols):
            try:
                row_idx = i // cols
                col_idx = i % cols
                
                shape = f['0'][str(cur_level)][str(i)][()].shape
                if len(shape) == 4:
                    shape = shape[:-1]  # 移除最后一个维度
                
                row_heights[row_idx] = max(row_heights[row_idx], shape[1])
                col_widths[col_idx] = max(col_widths[col_idx], shape[2])
            except KeyError:
                print(f"Warning: Block {i} not found, using sample shape")
                row_idx = i // cols
                col_idx = i % cols
                row_heights[row_idx] = max(row_heights[row_idx], sample_shape[1])
                col_widths[col_idx] = max(col_widths[col_idx], sample_shape[2])
        
        total_height = int(row_heights.sum())
        total_width = int(col_widths.sum())
        
        # 创建3D shape
        full_shape = (sample_shape[0], total_height, total_width)
            
        return full_shape, row_heights, col_widths

def process_h5_to_v3draw(input_file, output_folder, chunk_size_z=16):
    """
    优化版H5到v3draw转换，使用顺序处理和更小的Z块
    
    Args:
        input_file: 输入H5文件路径
        output_folder: 输出文件夹路径
        chunk_size_z: Z方向的块大小，默认16
    """
    filename = os.path.basename(input_file)
    # 移除 .h5 或 .pyramid.h5 扩展名，保留基础文件名
    if filename.endswith('.pyramid.h5'):
        base_filename = filename[:-11]  # 移除 '.pyramid.h5'
    elif filename.endswith('.h5'):
        base_filename = filename[:-3]   # 移除 '.h5'
    else:
        # 如果没有这些扩展名，使用原始逻辑
        spRes = filename.split(".")
        base_filename = spRes[0] if len(spRes) > 0 else filename

    out_file = os.path.join(output_folder, base_filename + ".v3draw")
    print(f"Output file: {out_file}")
    
    if not os.path.exists(os.path.dirname(out_file)):
        os.makedirs(os.path.dirname(out_file))

    if os.path.exists(out_file):
        print("V3draw file already existed")
        return out_file

    shapes, arrangements = get_shape_from_h5file(input_file)
    cur_level = str(max([int(key) for key in shapes]))
    
    cols, rows = arrangements[cur_level]
    total_blocks = rows * cols
    
    # 预计算维度信息
    print("Calculating dimensions...")
    full_shape, row_heights, col_widths = calculate_dimensions(
        input_file, cur_level, rows, cols)
    
    print(f"Full image shape: {full_shape}")
    mem_info = monitor_memory()
    print(f"Memory status: {mem_info['used_gb']:.1f}GB used, {mem_info['available_gb']:.1f}GB available")
    
    Z, Y, X = full_shape
    
    # 写入v3d文件头，格式为(1, Z, Y, X) MIP兼容格式
    print(f"Writing v3d header for shape: (1, {Z}, {X}, {Y})")
    if not write_v3d_header(out_file, (Y, X, Z, 1), datatype=2):
        raise Exception("Failed to write v3d header")
    
    # 按Z切片处理，减少内存占用
    total_z_chunks = (Z + chunk_size_z - 1) // chunk_size_z
    
    print(f"Processing in {total_z_chunks} Z-chunks of size {chunk_size_z}")
    
    for z_chunk_idx in range(total_z_chunks):
        z_start = z_chunk_idx * chunk_size_z
        z_end = min(z_start + chunk_size_z, Z)
        current_z_size = z_end - z_start
        
        print(f"\rProcessing Z chunk {z_chunk_idx + 1}/{total_z_chunks}: Z[{z_start}:{z_end}]", end='', flush=True)
        
        # 为当前Z切片创建临时数组，只包含当前处理的Z层
        z_chunk_data = np.zeros((current_z_size, Y, X), dtype=np.uint16)
        
        # 顺序处理每个块
        with h5py.File(input_file, 'r') as f:
            for idx in range(total_blocks):
                row_idx = idx // cols
                col_idx = idx % cols
                
                try:
                    # 读取图像数据
                    img = f['0'][str(cur_level)][str(idx)][()]
                    
                    # 确保是numpy数组
                    if not isinstance(img, np.ndarray):
                        img = np.array(img)
                    
                    # 如果是4D数组，合并所有通道（求和）
                    if len(img.shape) == 4:
                        img = np.sum(img, axis=3)
                    
                    # 限制z范围
                    if z_end <= img.shape[0]:
                        img_chunk = img[z_start:z_end]
                    else:
                        # 如果图像的z维度小于请求的范围，需要处理
                        actual_z_start = min(z_start, img.shape[0])
                        actual_z_end = min(z_end, img.shape[0])
                        if actual_z_start < actual_z_end:
                            img_chunk = img[actual_z_start:actual_z_end]
                        else:
                            continue  # 跳过这个块
                    
                    # 计算在全图中的位置
                    y_start = 0
                    for r in range(row_idx):
                        y_start += row_heights[r]
                    y_end = y_start + img_chunk.shape[1]
                    
                    x_start = 0
                    for c in range(col_idx):
                        x_start += col_widths[c]
                    x_end = x_start + img_chunk.shape[2]
                    
                    # 计算在当前z分片中的相对位置
                    rel_z_start = 0
                    rel_z_end = img_chunk.shape[0]
                    
                    # 将块复制到目标位置
                    z_chunk_data[rel_z_start:rel_z_end, y_start:y_end, x_start:x_end] = img_chunk
                    
                    # 立即释放块内存
                    del img, img_chunk
                    
                except Exception as e:
                    print(f"\nError processing block {idx}: {str(e)}")
        
        # 转换为v3d格式: (Z, Y, X) -> (1, Z, Y, X)
        chunk_to_write = np.array([z_chunk_data], dtype=np.uint16)  # (1, Z, Y, X)
        
        # 写入当前Z切片到文件
        if not append_v3d_chunk(out_file, chunk_to_write):
            raise Exception(f"Failed to write Z chunk {z_chunk_idx}")
        
        # 释放内存
        del z_chunk_data, chunk_to_write
        gc.collect()
        
        # 显示内存使用情况
        memory_gb = get_memory_usage() / 1024
        print(f" [Memory: {memory_gb:.1f}GB]", flush=True)
    
    print(f"\nDirect write completed: {out_file}")
    return out_file

def get_shape_from_h5file(input_file: str):
    """
    获取H5文件中的形状和排列信息
    """
    with h5py.File(input_file, 'r') as f:
        try:
            base_shape = f['0']['0']['0'][()].shape
        except (KeyError, TypeError):
            # 如果无法读取第一个块，尝试其他方法
            print("Warning: Cannot read first block, trying alternative method")
            levels = list(f['0'].keys())
            if levels:
                first_level = levels[0]
                blocks = list(f['0'][first_level].keys())
                if blocks:
                    base_shape = f['0'][first_level][blocks[0]][()].shape
                else:
                    raise ValueError("No blocks found in H5 file")
            else:
                raise ValueError("No levels found in H5 file")
                
        base_score = np.log(base_shape[1]/base_shape[2])
        shapes = {}
        arrangements = {}
        
        for level in f['0']:
            try:
                level_group = f['0'][level]
                size = len(level_group)
                
                # 乘法分解
                arranges = factor_pairs(size)
                min_score = 1e9
                arrange = (0, 0)
                target_shape = (0, 0, 0)
                
                for row, col in arranges:
                    try:
                        size0 = f['0'][level]['0'][()].shape[0]
                        size1 = sum(f['0'][level][str(i)][()].shape[1] for i in range(col))
                        size2 = sum(f['0'][level][str(i)][()].shape[2]
                                    for i in range(0, size, col))
                        score = abs(np.log(size1/size2)-base_score)
                        if score < min_score:
                            min_score = score
                            shape = (size0, size1, size2)
                            arrange = (row, col)
                    except (KeyError, IndexError, ZeroDivisionError):
                        continue
                        
                shapes[level] = shape
                arrangements[level] = arrange
            except (KeyError, TypeError):
                print(f"Warning: Cannot process level {level}")
                continue
                
        return shapes, arrangements

def factor_pairs(n):
    if n < 1:
        raise ValueError("Input must be a positive integer.")

    pairs = []
    for i in range(1, int(n ** 0.5) + 1):
        if n % i == 0:
            pairs.append((i, n // i))
            if i != n // i:
                pairs.append((n // i, i))
    return pairs

def Batch_H5ToV3draw(input_dir, output_folder=None, chunk_size_z=50):
    """
    批量转换指定目录下的所有H5文件到v3draw格式
    
    Parameters:
    -----------
    input_dir : str
        输入目录，包含待处理的H5文件
    output_folder : str
        输出目录，v3draw文件的保存路径
    chunk_size_z : int, optional
        Z轴分片大小，默认50
    """
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.endswith('.h5'):
                file_path = os.path.join(root, file)
                try:
                    print(f"process_h5_to_v3draw: {file_path}")
                    # 使用更小的Z块大小来减少内存占用
                    process_h5_to_v3draw(file_path, output_folder, chunk_size_z=chunk_size_z)
                    print("gc collect begin")
                    gc.collect()
                    print("gc collect end")
                except Exception as e:
                    errorMessage = "Error When Convert " + file_path
                    print(e)
                    print(errorMessage)
                    print("gc collect begin")
                    gc.collect()
                    print("gc collect end")
                print('-------------------------------')


def Single_H5ToV3draw(input_file, output_folder=None, chunk_size_z=50):
    """
    单个H5文件转换入口函数

    Parameters:
    -----------
    input_file : str
        输入H5文件路径
    output_folder : str, optional
        输出目录路径，如果为None则使用输入文件所在目录
    chunk_size_z : int, optional
        Z轴分片大小，默认50
    """
    # 如果没有指定输出目录，则使用输入文件所在目录
    if output_folder is None:
        output_folder = os.path.dirname(input_file)

    return process_h5_to_v3draw(input_file, output_folder, chunk_size_z=chunk_size_z)

if __name__ == "__main__":
    # python3 {script_path} --image-path "{image_path}" --output-path "{output_path}"
    # 解析参数 --image-path 和 --output-path
    import argparse
    parser = argparse.ArgumentParser(description="Convert H5 images to v3draw format.")
    parser.add_argument('--image-path', type=str, required=True, help='Path to the H5 image file or directory.')
    args = parser.parse_args()

    input_path = args.image_path.strip()
    
    if os.path.isdir(input_path):
        Batch_H5ToV3draw(input_path)
    else:
        # 单个文件处理
        Single_H5ToV3draw(input_path)