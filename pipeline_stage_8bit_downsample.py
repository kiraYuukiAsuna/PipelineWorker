from v3d_io import load_v3d_raw_img_file, save_v3d_raw_img_file
import numpy as np
import os
import struct
import gc
import psutil


def get_memory_usage():
    """获取当前内存使用情况(MB)"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def read_v3d_header_info(filename):
    """读取v3d文件头信息"""
    try:
        with open(filename, 'rb') as f_obj:
            # 读取格式键
            len_formatkey = len('raw_image_stack_by_hpeng')
            formatkey = f_obj.read(len_formatkey)
            formatkey = struct.unpack(str(len_formatkey) + 's', formatkey)
            if formatkey[0] != b'raw_image_stack_by_hpeng':
                return None

            # 读取字节序
            endiancode = f_obj.read(1)
            endiancode = struct.unpack('c', endiancode)[0]
            
            # 读取数据类型
            datatype = f_obj.read(2)
            if endiancode == b'L':
                datatype = struct.unpack('<h', datatype)[0]
            else:
                datatype = struct.unpack('>h', datatype)[0]

            # 读取图像尺寸
            size = f_obj.read(4 * 4)
            if endiancode == b'L':
                size = struct.unpack('<4l', size)
            else:
                size = struct.unpack('>4l', size)

            return {
                'endian': endiancode,
                'datatype': datatype,
                'size': size,
                'header_size': f_obj.tell()
            }
    except Exception as e:
        print(f"Error reading header: {e}")
        return None


def load_v3d_chunk(filename, header_info, z_start, z_end):
    """加载v3d文件的指定Z切片范围"""
    size = header_info['size']  # size = (X, Y, Z, C) 从文件头读取
    datatype = header_info['datatype']
    
    # 计算数据类型大小
    if datatype == 1:
        dtype = np.uint8
        bytes_per_pixel = 1
    elif datatype == 2:
        dtype = np.uint16
        bytes_per_pixel = 2
    else:
        dtype = np.float32
        bytes_per_pixel = 4
    
    # v3d文件格式: 存储顺序是 (C, Z, Y, X)
    # size从文件头读取的顺序是 (X, Y, Z, C)
    
    # 计算每个Z切片在每个通道中的像素数和字节数
    pixels_per_z_per_channel = size[0] * size[1]  # X * Y
    bytes_per_z_per_channel = pixels_per_z_per_channel * bytes_per_pixel
    
    # 计算要读取的切片数
    num_slices = z_end - z_start
    
    try:
        with open(filename, 'rb') as f_obj:
            # 跳过文件头
            f_obj.seek(header_info['header_size'])
            
            # v3d存储格式: 所有通道的第0层，然后所有通道的第1层，依此类推
            # 跳过前面的Z切片（所有通道）
            if z_start > 0:
                skip_bytes = z_start * bytes_per_z_per_channel * size[3]
                f_obj.seek(skip_bytes, 1)
            
            # 读取指定Z范围的数据（所有通道）
            read_bytes = num_slices * bytes_per_z_per_channel * size[3]
            data_bytes = f_obj.read(read_bytes)
            
            # 转换为numpy数组
            data_1d = np.frombuffer(data_bytes, dtype=dtype)
            
            # 重塑为v3d存储格式: (C, Z, Y, X) = (size[3], num_slices, size[1], size[0])
            data = data_1d.reshape((size[3], num_slices, size[1], size[0]))
            
            # 与v3d_io.py保持一致的轴变换: (C, Z, Y, X) -> (Y, X, Z, C)
            data = np.moveaxis(data, 0, -1)  # C轴移到最后: (Z, Y, X, C)
            data = np.moveaxis(data, 0, -2)  # Z轴移到倒数第二: (Y, X, Z, C)
            
            return data
            
    except Exception as e:
        print(f"Error loading chunk: {e}")
        return None


def downsample_chunk(chunk_data, factors, method='mean'):
    """对数据块进行下采样"""
    original_shape = chunk_data.shape
    
    # 计算新的形状
    new_shape = [
        original_shape[0] // factors[0],  # Y
        original_shape[1] // factors[1],  # X  
        original_shape[2] // factors[2],  # Z
        original_shape[3]                 # C
    ]
    
    # 修剪到可被因子整除的大小
    trimmed_shape = [
        new_shape[0] * factors[0],
        new_shape[1] * factors[1], 
        new_shape[2] * factors[2],
        original_shape[3]
    ]
    
    if list(original_shape) != trimmed_shape:
        chunk_data = chunk_data[:trimmed_shape[0], :trimmed_shape[1], :trimmed_shape[2], :]
    
    # 为每个通道进行下采样
    downsampled_data = np.zeros(new_shape, dtype=chunk_data.dtype)
    
    for c in range(original_shape[3]):
        channel_data = chunk_data[..., c]
        
        # 重塑数据以便下采样
        reshaped = channel_data.reshape(
            new_shape[0], factors[0],
            new_shape[1], factors[1], 
            new_shape[2], factors[2]
        )
        
        # 应用下采样方法
        if method == 'mean':
            downsampled_data[..., c] = reshaped.mean(axis=(1, 3, 5))
        elif method == 'max':
            downsampled_data[..., c] = reshaped.max(axis=(1, 3, 5))
        elif method == 'min':
            downsampled_data[..., c] = reshaped.min(axis=(1, 3, 5))
        elif method == 'nearest':
            downsampled_data[..., c] = reshaped[:, 0, :, 0, :, 0]
        else:
            print(f"Unknown method: {method}. Using 'mean'.")
            downsampled_data[..., c] = reshaped.mean(axis=(1, 3, 5))
    
    return downsampled_data


def write_v3d_header(output_file, header_info):
    """写入v3d文件头"""
    with open(output_file, 'wb') as f_obj:
        # 写入格式键
        formatkey = b'raw_image_stack_by_hpeng'
        f_obj.write(struct.pack('<24s', formatkey))
        
        # 写入字节序
        f_obj.write(struct.pack('<s', b'L'))
        
        # 写入数据类型
        f_obj.write(struct.pack('<h', header_info['datatype']))
        
        # 写入新的图像尺寸
        # v3d文件头的尺寸顺序是(X, Y, Z, C)，new_size已经是这个顺序
        new_size = header_info['new_size']
        f_obj.write(struct.pack('<4l', new_size[0], new_size[1], new_size[2], new_size[3]))


def append_v3d_chunk(output_file, chunk_data):
    """将数据块追加到v3d文件"""
    with open(output_file, 'ab') as f_obj:
        # 输入: chunk_data 格式为 (Y, X, Z, C)
        # v3d存储格式需要: (C, Z, Y, X)
        
        # 按v3d格式要求的顺序写入：先所有通道的第0层，再所有通道的第1层...
        num_z = chunk_data.shape[2]
        num_channels = chunk_data.shape[3]
        
        # 转换轴顺序: (Y, X, Z, C) -> (C, Z, Y, X)
        # 方法1: 使用moveaxis
        reordered_data = np.moveaxis(chunk_data, -2, 0)  # Z轴移到前面: (Y, X, C, Z) -> (Z, Y, X, C)
        reordered_data = np.moveaxis(reordered_data, -1, 0)  # C轴移到最前: (Z, Y, X, C) -> (C, Z, Y, X)
        
        # 直接写入重排序后的数据
        f_obj.write(reordered_data.tobytes())


def downsample_v3d_image_chunked(input_filename, output_filename=None, 
                                factors=(2, 2, 2), method='mean', 
                                chunk_size_z=50, use_chunked=True):
    """
    使用分片模式进行v3d图像下采样，降低内存占用
    
    修复内容：
    1. 修正了v3d格式的轴顺序处理
    2. 优化了分片写入逻辑，避免条纹问题
    3. 确保分片边界与下采样因子对齐
    
    Parameters:
    -----------
    input_filename : str
        输入v3d raw图像文件路径
    output_filename : str, optional
        输出下采样图像文件路径
    factors : tuple, optional
        各空间维度的下采样因子 (x, y, z)，默认(2, 2, 2)
    method : str, optional
        下采样方法: 'mean', 'max', 'min', 'nearest'
    chunk_size_z : int, optional
        Z轴分片大小，默认50
    use_chunked : bool, optional
        是否使用分片模式
    
    Returns:
    --------
    bool: 处理是否成功
    """
    print(f"开始处理文件: {input_filename}")
    print(f"分片模式: {'启用' if use_chunked else '禁用'}")
    print(f"下采样因子: {factors}")
    print(f"下采样方法: {method}")
    
    if use_chunked:
        print(f"Z轴分片大小: {chunk_size_z}")
    
    initial_memory = get_memory_usage()
    print(f"初始内存使用: {initial_memory:.0f}MB")
    
    if not use_chunked:
        # 使用原始方法
        return downsample_v3d_image_efficient_original(input_filename, output_filename, factors, method)
    
    try:
        # 读取文件头信息
        header_info = read_v3d_header_info(input_filename)
        if not header_info:
            print("Error: 无法读取文件头")
            return False
        
        original_size = header_info['size']  # (X, Y, Z, C)
        print(f"原始图像尺寸: {original_size} (X, Y, Z, C)")
        
        # 转换因子格式 (x, y, z) -> 对应到size的 (0, 1, 2)
        if len(factors) == 3:
            # factors是(x, y, z)格式，对应size的(0, 1, 2)
            ds_factors_by_size = (factors[0], factors[1], factors[2], 1)  # (X, Y, Z, C)
        else:
            ds_factors_by_size = factors
        
        # 计算下采样后的尺寸 (按size的顺序: X, Y, Z, C)
        new_size = [
            original_size[0] // ds_factors_by_size[0],  # X
            original_size[1] // ds_factors_by_size[1],  # Y
            original_size[2] // ds_factors_by_size[2],  # Z
            original_size[3]                            # C
        ]
        
        header_info['new_size'] = new_size
        print(f"下采样后尺寸: {new_size} (X, Y, Z, C)")
        
        # 为了与downsample_chunk兼容，我们还需要计算 (Y, X, Z, C) 格式的因子
        ds_factors_yxzc = (ds_factors_by_size[1], ds_factors_by_size[0], ds_factors_by_size[2], ds_factors_by_size[3])
        print(f"下采样因子 (Y, X, Z, C): {ds_factors_yxzc}")
        
        # 设置输出文件名
        if output_filename is None:
            base, ext = os.path.splitext(input_filename)
            output_filename = f"{base}_downsampled{ext}"
        
        # 写入文件头
        write_v3d_header(output_filename, header_info)
        
        # 关键修复：确保分片大小与下采样因子对齐
        z_factor = ds_factors_by_size[2]
        aligned_chunk_size = (chunk_size_z // z_factor) * z_factor
        if aligned_chunk_size == 0:
            aligned_chunk_size = z_factor
        
        # 计算有效的Z轴范围（确保能被下采样因子整除）
        effective_z_size = (original_size[2] // z_factor) * z_factor
        print(f"有效Z轴大小: {effective_z_size} (原始: {original_size[2]})")
        if aligned_chunk_size != chunk_size_z:
            print(f"分片大小调整: {chunk_size_z} -> {aligned_chunk_size} (对齐到下采样因子)")
        
        # 分片处理
        total_chunks = 0
        processed_z = 0
        
        for z_start in range(0, effective_z_size, aligned_chunk_size):
            z_end = min(z_start + aligned_chunk_size, effective_z_size)
            current_chunk_size = z_end - z_start
            
            print(f"\r处理Z切片 {z_start}-{z_end-1} ({current_chunk_size}层)", end='', flush=True)
            
            # 加载当前分片
            chunk_data = load_v3d_chunk(input_filename, header_info, z_start, z_end)
            if chunk_data is None:
                print(f"\n错误: 无法加载Z切片 {z_start}-{z_end}")
                return False
            
            # 下采样当前分片
            downsampled_chunk = downsample_chunk(chunk_data, ds_factors_yxzc, method)
            
            # 释放原始分片数据
            del chunk_data
            gc.collect()
            
            # 追加下采样后的分片到输出文件
            append_v3d_chunk(output_filename, downsampled_chunk)
            
            # 释放下采样分片数据
            del downsampled_chunk
            gc.collect()
            
            processed_z += current_chunk_size
            total_chunks += 1
            
            # 显示内存使用情况
            current_memory = get_memory_usage()
            if total_chunks % 5 == 0:  # 每5个分片显示一次
                print(f" [内存: {current_memory:.0f}MB]", end='')
        
        final_memory = get_memory_usage()
        print(f"\n处理完成！")
        print(f"输出文件: {output_filename}")
        print(f"处理的分片数: {total_chunks}")
        print(f"最终内存使用: {final_memory:.0f}MB")
        print(f"内存增长: {final_memory - initial_memory:.0f}MB")
        
        return True
        
    except Exception as e:
        error_memory = get_memory_usage()
        print(f"\n处理失败: {e}")
        print(f"错误时内存使用: {error_memory:.0f}MB")
        return False


def downsample_v3d_image_efficient_original(input_filename, output_filename=None, 
                                          factors=(2, 2, 2), method='mean'):
    """
    原始的下采样方法（一次性加载全部数据）
    """
    print("使用原始方法处理...")
    
    # Load the image
    print(f"Loading image: {input_filename}")
    im = load_v3d_raw_img_file(input_filename)
    
    if not im:
        print(f"Error loading file: {input_filename}")
        return None
    
    # Get image data and shape
    data = im['data']
    original_shape = data.shape
    print(f"Original image shape: {original_shape}")
    
    # Ensure the factors are valid
    if len(factors) == 3:
        # Convert factors from (x, y, z) to (y, x, z, c) format
        factors = (factors[1], factors[0], factors[2], 1)
    
    # Calculate new dimensions
    new_shape = [
        original_shape[0] // factors[0],
        original_shape[1] // factors[1],
        original_shape[2] // factors[2],
        original_shape[3]  # channels are not downsampled
    ]
    
    print(f"Downsampling with factors: {factors[:3]}")
    print(f"New shape will be: {new_shape}")
    
    # Trim data to be evenly divisible by factors
    trimmed_shape = [
        new_shape[0] * factors[0],
        new_shape[1] * factors[1],
        new_shape[2] * factors[2],
        original_shape[3]
    ]
    
    if original_shape != tuple(trimmed_shape):
        print(f"Trimming image from {original_shape} to {trimmed_shape}")
        data = data[:trimmed_shape[0], :trimmed_shape[1], :trimmed_shape[2], :]
    
    # Create output array with proper data type
    if im['datatype'] == 1:
        downsampled_data = np.zeros(new_shape, dtype=np.uint8)
    elif im['datatype'] == 2:
        downsampled_data = np.zeros(new_shape, dtype=np.uint16)
    else:
        downsampled_data = np.zeros(new_shape, dtype=np.float32)
    
    # Perform efficient downsampling for each channel
    print(f"Performing downsampling using method: {method}")
    for c in range(original_shape[3]):
        # Extract the current channel
        channel_data = data[..., c]
        
        # Reshape to group elements that will be combined
        reshaped = channel_data.reshape(
            new_shape[0], factors[0], 
            new_shape[1], factors[1], 
            new_shape[2], factors[2]
        )
        
        # Apply downsampling method
        if method == 'mean':
            downsampled_data[..., c] = reshaped.mean(axis=(1, 3, 5))
        elif method == 'max':
            downsampled_data[..., c] = reshaped.max(axis=(1, 3, 5))
        elif method == 'min':
            downsampled_data[..., c] = reshaped.min(axis=(1, 3, 5))
        elif method == 'nearest':
            downsampled_data[..., c] = reshaped[:, 0, :, 0, :, 0]
        else:
            print(f"Unknown method: {method}. Using 'mean'.")
            downsampled_data[..., c] = reshaped.mean(axis=(1, 3, 5))
    
    # Create a new image dictionary for downsampled data
    im_downsampled = im.copy()
    im_downsampled['data'] = downsampled_data
    im_downsampled['size'] = downsampled_data.shape
    
    # Save the downsampled image
    if output_filename is None:
        base, ext = os.path.splitext(input_filename)
        output_filename = f"{base}_downsampled{ext}"
    
    print(f"Saving downsampled image to: {output_filename}")
    save_v3d_raw_img_file(im_downsampled, output_filename)
    print(f"Downsampling complete. Original size: {original_shape}, New size: {downsampled_data.shape}")
    
    return im_downsampled


def downsample_v3d_image_efficient(input_filename, output_filename, 
                                  factors=(2, 2, 2), method='mean', 
                                  use_chunked=True, chunk_size_z=50):
    """
    高效下采样v3d图像的包装函数
    
    Parameters:
    -----------
    input_filename : str
        输入v3d raw图像文件路径
    output_filename : str, optional
        输出下采样图像文件路径
    factors : tuple, optional
        各空间维度的下采样因子 (x, y, z)，默认(2, 2, 2)
    method : str, optional
        下采样方法: 'mean', 'max', 'min', 'nearest'
    use_chunked : bool, optional
        是否使用分片模式（推荐用于大文件）
    chunk_size_z : int, optional
        Z轴分片大小，仅在use_chunked=True时有效
    
    Returns:
    --------
    dict or bool
        如果use_chunked=False，返回图像字典；如果use_chunked=True，返回成功状态
    """
    if use_chunked:
        success = downsample_v3d_image_chunked(input_filename, output_filename, factors, method, chunk_size_z, True)
        return success
    else:
        return downsample_v3d_image_efficient_original(input_filename, output_filename, factors, method)


def Batch_DownsampleV3D(input_dir, factors=(4, 4, 2), method='mean', use_chunked=True, chunk_size_z=50):
    """
    批量下采样指定目录下的所有v3d图像文件
    
    Parameters:
    -----------
    input_dir : str
        输入目录，包含待处理的v3d图像文件
    factors : tuple, optional
        各空间维度的下采样因子 (x, y, z)，默认(2, 2, 2)
    method : str, optional
        下采样方法: 'mean', 'max', 'min', 'nearest'
    use_chunked : bool, optional
        是否使用分片模式（推荐用于大文件）
    chunk_size_z : int, optional
        Z轴分片大小，仅在use_chunked=True时有效
    """
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.endswith('_8bit.v3draw'):
                input_file = os.path.join(root, file)
                base_name = file.split('.')[0]
                output_file = os.path.join(root, f"{base_name}_downsampled.v3draw")

                if os.path.exists(output_file):
                    print(f"{output_file} exist, skip")
                    continue
                
                # 使用分片模式（推荐，内存占用低）
                success = downsample_v3d_image_efficient(
                    input_file, 
                    output_file,
                    factors=factors,
                    method=method,
                    use_chunked=use_chunked,
                    chunk_size_z=chunk_size_z
                )
                
                if success:
                    print("下采样成功完成！")
                else:
                    print("下采样失败！")


def Single_Downsample(input_file, output_file=None, 
                      factors=(4, 4, 2), method='mean', 
                      use_chunked=True, chunk_size_z=50):
    """
    单个文件下采样入口函数

    Parameters:
    -----------
    input_file : str
        输入v3d raw图像文件路径
    output_file : str, optional
        输出下采样图像文件路径
    factors : tuple, optional
        各空间维度的下采样因子 (x, y, z)，默认(2, 2, 2)
    method : str, optional
        下采样方法: 'mean', 'max', 'min', 'nearest'
    use_chunked : bool, optional
        是否使用分片模式（推荐用于大文件）
    chunk_size_z : int, optional
        Z轴分片大小，仅在use_chunked=True时有效
    """

    # 如果没有指定输出文件名，则生成默认输出文件名
    if output_file is None:
        directory = os.path.dirname(input_file)
        filename = os.path.basename(input_file)
        base_name = filename.split('.')[0]
        output_file = os.path.join(directory, f"{base_name}_downsampled.v3draw")

    return downsample_v3d_image_efficient(
        input_file,
        output_file,
        factors=factors,
        method=method,
        use_chunked=use_chunked,
        chunk_size_z=chunk_size_z
    )

if __name__ == "__main__":
    # python3 {script_path} --image-path "{image_path}"
    # 解析参数 --image-path
    import argparse
    parser = argparse.ArgumentParser(description="Downsample v3d images.")
    parser.add_argument('--image-path', type=str, required=True, help='Path to the v3d image file or directory.')
    args = parser.parse_args()

    input_path = args.image_path
    input_path = input_path.strip()
    if os.path.isdir(input_path):
        # 批量处理目录下的所有v3d文件
        Batch_DownsampleV3D(input_path)
    else:
        # 单个文件处理
        Single_Downsample(input_path)
