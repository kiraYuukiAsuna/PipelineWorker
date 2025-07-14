from v3d_io import load_v3d_raw_img_file, save_v3d_raw_img_file
import numpy as np
import os
import struct


def read_v3d_header(filename):
    """
    读取v3d文件头信息，不加载图像数据
    
    Returns:
    --------
    dict: 包含文件头信息的字典，或None如果出错
    """
    try:
        with open(filename, 'rb') as f_obj:
            # 读取格式键
            len_formatkey = len('raw_image_stack_by_hpeng')
            formatkey = f_obj.read(len_formatkey)
            formatkey = struct.unpack(str(len_formatkey) + 's', formatkey)
            if formatkey[0] != b'raw_image_stack_by_hpeng':
                print("ERROR: File unrecognized (not raw, v3draw) or corrupted.")
                return None

            # 读取字节序
            endiancode = f_obj.read(1)
            endiancode = struct.unpack('c', endiancode)[0]
            if endiancode != b'B' and endiancode != b'L':
                print("ERROR: Only supports big- or little- endian.")
                return None

            # 读取数据类型
            datatype = f_obj.read(2)
            if endiancode == b'L':
                datatype = struct.unpack('<h', datatype)[0]
            else:
                datatype = struct.unpack('>h', datatype)[0]
            
            if datatype < 1 or datatype > 4:
                print(f"ERROR: Unrecognized data type code [{datatype}].")
                return None

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


def process_chunk_minmax_scan(filename, header_info, chunk_size_mb=512):
    """
    分片扫描文件以找到最小值和最大值（用于minmax缩放）
    
    Parameters:
    -----------
    filename : str
        输入文件路径
    header_info : dict
        文件头信息
    chunk_size_mb : int
        每个分片的大小（MB）
    
    Returns:
    --------
    tuple: (min_val, max_val)
    """
    size = header_info['size']
    datatype = header_info['datatype']
    endian = header_info['endian']
    
    # 计算总像素数和字节数
    total_pixels = size[0] * size[1] * size[2] * size[3]
    bytes_per_pixel = 2 if datatype == 2 else (1 if datatype == 1 else 4)
    total_bytes = total_pixels * bytes_per_pixel
    
    # 计算分片大小（以像素为单位）
    chunk_bytes = chunk_size_mb * 1024 * 1024
    chunk_pixels = chunk_bytes // bytes_per_pixel
    
    min_val = float('inf')
    max_val = float('-inf')
    
    print(f"扫描文件以获取最小/最大值，使用 {chunk_size_mb}MB 分片...")
    
    with open(filename, 'rb') as f_obj:
        # 跳过文件头
        f_obj.seek(header_info['header_size'])
        
        processed_pixels = 0
        chunk_count = 0
        
        while processed_pixels < total_pixels:
            # 计算当前分片的像素数
            current_chunk_pixels = min(chunk_pixels, total_pixels - processed_pixels)
            current_chunk_bytes = current_chunk_pixels * bytes_per_pixel
            
            # 读取数据
            chunk_data = f_obj.read(current_chunk_bytes)
            if not chunk_data:
                break
            
            # 转换为numpy数组
            if datatype == 2:
                chunk_array = np.frombuffer(chunk_data, np.uint16)
            elif datatype == 1:
                chunk_array = np.frombuffer(chunk_data, np.uint8)
            else:
                chunk_array = np.frombuffer(chunk_data, np.float32)
            
            # 更新最小值和最大值
            chunk_min = float(np.min(chunk_array))
            chunk_max = float(np.max(chunk_array))
            min_val = min(min_val, chunk_min)
            max_val = max(max_val, chunk_max)
            
            processed_pixels += current_chunk_pixels
            chunk_count += 1
            
            # 显示进度
            progress = (processed_pixels / total_pixels) * 100
            print(f"\r扫描进度: {progress:.1f}% (分片 {chunk_count})", end='', flush=True)
    
    print(f"\n扫描完成。最小值: {min_val}, 最大值: {max_val}")
    return min_val, max_val


def convert_16bit_to_8bit_chunked(input_filename, output_filename=None, scaling_method='minmax', chunk_size_mb=512):
    """
    使用分片加载模式将16位图像转换为8位图像，降低内存占用
    
    Parameters:
    -----------
    input_filename : str
        输入v3d raw图像文件路径
    output_filename : str, optional
        输出8位图像文件路径。如果为None，将在输入文件名后添加"_8bit"
    scaling_method : str, optional
        16位到8位的缩放方法:
        - 'minmax': 基于数据中的实际最小/最大值进行缩放
        - 'full': 假设完整的16位范围(0-65535)进行缩放
        - 'clip': 简单地截断高位
    chunk_size_mb : int, optional
        每个分片的大小（MB），默认512MB
    
    Returns:
    --------
    bool: 转换是否成功
    """
    print(f"开始处理文件: {input_filename}")
    print(f"使用分片大小: {chunk_size_mb}MB")
    
    # 读取文件头
    header_info = read_v3d_header(input_filename)
    if not header_info:
        print("Error reading file header")
        return False
    
    # 检查是否为16位图像
    if header_info['datatype'] != 2:
        print(f"输入图像不是16位。当前数据类型: {header_info['datatype']}")
        return False
    
    # 设置输出文件名
    if output_filename is None:
        base, ext = os.path.splitext(input_filename)
        output_filename = f"{base}_8bit{ext}"
    
    size = header_info['size']
    endian = header_info['endian']
    total_pixels = size[0] * size[1] * size[2] * size[3]
    
    # 如果使用minmax方法，先扫描文件获取最小/最大值
    min_val = 0.0
    max_val = 0.0
    scale_factor = 1.0
    offset = 0.0
    
    if scaling_method == 'minmax':
        min_val, max_val = process_chunk_minmax_scan(input_filename, header_info, chunk_size_mb)
        if max_val == min_val:
            print("警告: 图像中所有像素值相同，输出将为全零图像")
            scale_factor = 0.0
            offset = min_val
        else:
            scale_factor = 255.0 / (max_val - min_val)
            offset = min_val
    
    # 计算分片大小
    chunk_bytes = chunk_size_mb * 1024 * 1024
    chunk_pixels = chunk_bytes // 2  # 16位 = 2字节每像素
    
    # 写入输出文件
    print("开始转换和写入...")
    
    try:
        with open(input_filename, 'rb') as input_file, open(output_filename, 'wb') as output_file:
            # 写入输出文件头
            formatkey = b'raw_image_stack_by_hpeng'
            output_file.write(struct.pack('<24s', formatkey))
            output_file.write(struct.pack('<s', b'L'))  # 小端
            output_file.write(struct.pack('<h', 1))  # 8位数据类型
            output_file.write(struct.pack('<4l', size[0], size[1], size[2], size[3]))
            
            # 跳过输入文件头
            input_file.seek(header_info['header_size'])
            
            processed_pixels = 0
            chunk_count = 0
            
            while processed_pixels < total_pixels:
                # 计算当前分片大小
                current_chunk_pixels = min(chunk_pixels, total_pixels - processed_pixels)
                current_chunk_bytes = current_chunk_pixels * 2  # 2字节每像素（16位）
                
                # 读取16位数据
                chunk_data = input_file.read(current_chunk_bytes)
                if not chunk_data:
                    break
                
                # 转换为numpy数组
                chunk_16bit = np.frombuffer(chunk_data, np.uint16)
                
                # 转换为8位
                if scaling_method == 'minmax':
                    if max_val == min_val:
                        chunk_8bit = np.zeros_like(chunk_16bit, dtype=np.uint8)
                    else:
                        chunk_8bit = np.uint8((chunk_16bit.astype(np.float32) - offset) * scale_factor)
                elif scaling_method == 'full':
                    chunk_8bit = np.uint8(chunk_16bit.astype(np.float32) / 256.0)
                elif scaling_method == 'clip':
                    chunk_8bit = np.uint8(chunk_16bit & 0xFF)
                else:
                    print(f"未知的缩放方法: {scaling_method}. 使用当前分片的minmax.")
                    # 对当前分片使用minmax方法
                    chunk_min = float(np.min(chunk_16bit))
                    chunk_max = float(np.max(chunk_16bit))
                    if chunk_max == chunk_min:
                        chunk_8bit = np.zeros_like(chunk_16bit, dtype=np.uint8)
                    else:
                        chunk_8bit = np.uint8(255.0 * (chunk_16bit.astype(np.float32) - chunk_min) / (chunk_max - chunk_min))
                
                # 写入8位数据
                output_file.write(chunk_8bit.tobytes())
                
                processed_pixels += current_chunk_pixels
                chunk_count += 1
                
                # 显示进度
                progress = (processed_pixels / total_pixels) * 100
                print(f"\r转换进度: {progress:.1f}% (分片 {chunk_count})", end='', flush=True)
        
        print(f"\n转换完成！已保存为: {output_filename}")
        return True
        
    except Exception as e:
        print(f"\n转换过程中出错: {e}")
        # 清理可能不完整的输出文件
        if os.path.exists(output_filename):
            os.remove(output_filename)
        return False


def convert_16bit_to_8bit(input_filename, output_filename=None, scaling_method='minmax', use_chunked=True, chunk_size_mb=512):
    """
    将16位图像转换为8位图像（包装函数，可选择是否使用分片模式）
    
    Parameters:
    -----------
    input_filename : str
        输入v3d raw图像文件路径
    output_filename : str, optional
        输出8位图像文件路径
    scaling_method : str, optional
        缩放方法: 'minmax', 'full', 'clip'
    use_chunked : bool, optional
        是否使用分片模式（推荐用于大文件）
    chunk_size_mb : int, optional
        分片大小（MB），仅在use_chunked=True时有效
    
    Returns:
    --------
    dict or bool
        如果use_chunked=False，返回图像字典；如果use_chunked=True，返回成功状态
    """
    if use_chunked:
        return convert_16bit_to_8bit_chunked(input_filename, output_filename, scaling_method, chunk_size_mb)
    else:
        # 原始方法（一次性加载全部数据）
        print("使用原始方法（一次性加载全部数据）...")
        im = load_v3d_raw_img_file(input_filename)
        
        if not im:
            print(f"Error loading file: {input_filename}")
            return None
        
        if im['datatype'] != 2:
            print(f"输入图像不是16位。当前数据类型: {im['datatype']}")
            return im
        
        im_8bit = im.copy()
        data_16bit = im['data']
        
        if scaling_method == 'minmax':
            min_val = np.min(data_16bit)
            max_val = np.max(data_16bit)
            if max_val == min_val:
                data_8bit = np.zeros_like(data_16bit, dtype=np.uint8)
            else:
                data_8bit = np.uint8(255.0 * (data_16bit - min_val) / (max_val - min_val))
        elif scaling_method == 'full':
            data_8bit = np.uint8(data_16bit / 256.0)
        elif scaling_method == 'clip':
            data_8bit = np.uint8(data_16bit & 0xFF)
        else:
            print(f"未知的缩放方法: {scaling_method}. 使用 'minmax'.")
            min_val = np.min(data_16bit)
            max_val = np.max(data_16bit)
            if max_val == min_val:
                data_8bit = np.zeros_like(data_16bit, dtype=np.uint8)
            else:
                data_8bit = np.uint8(255.0 * (data_16bit - min_val) / (max_val - min_val))
        
        im_8bit['datatype'] = 1
        im_8bit['data'] = data_8bit
        
        if output_filename is None:
            base, ext = os.path.splitext(input_filename)
            output_filename = f"{base}_8bit{ext}"
        
        save_v3d_raw_img_file(im_8bit, output_filename)
        print(f"转换完成并保存为: {output_filename}")
        return im_8bit


def Batch_Convert16bitTo8bit(input_dir, scaling_method='minmax', use_chunked=True, chunk_size_mb=512):
    """
    批量转换指定目录下的所有16位v3d图像文件为8位
    
    Parameters:
    -----------
    input_dir : str
        输入目录，包含待处理的v3d图像文件
    scaling_method : str, optional
        16位到8位的缩放方法:
        - 'minmax': 基于数据中的实际最小/最大值进行缩放
        - 'full': 假设完整的16位范围(0-65535)进行缩放
        - 'clip': 简单地截断高位
    use_chunked : bool, optional
        是否使用分片模式（推荐用于大文件）
    chunk_size_mb : int, optional
        分片大小（MB），仅在use_chunked=True时有效
    """
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.endswith('.v3draw') and not file.endswith('_8bit.v3draw'):
                input_file = os.path.join(root, file)
                base_name = file.split('.')[0]
                output_file = os.path.join(root, f"{base_name}_8bit.v3draw")

                if os.path.exists(output_file):
                    print(f"{output_file} exist, skip")
                    continue
                
                # 使用分片模式（推荐，内存占用低）
                success = convert_16bit_to_8bit(
                    input_file, 
                    output_file,
                    scaling_method=scaling_method,
                    use_chunked=use_chunked,
                    chunk_size_mb=chunk_size_mb
                )
                
                if success:
                    print("16位到8位转换成功完成！")
                else:
                    print("16位到8位转换失败！")


def Single_Convert16bitTo8bit(input_file, output_file=None, 
                             scaling_method='minmax', use_chunked=True, 
                             chunk_size_mb=512):
    """
    单个文件16位到8位转换入口函数

    Parameters:
    -----------
    input_file : str
        输入v3d raw图像文件路径
    output_file : str, optional
        输出8位图像文件路径
    scaling_method : str, optional
        16位到8位的缩放方法:
        - 'minmax': 基于数据中的实际最小/最大值进行缩放
        - 'full': 假设完整的16位范围(0-65535)进行缩放
        - 'clip': 简单地截断高位
    use_chunked : bool, optional
        是否使用分片模式（推荐用于大文件）
    chunk_size_mb : int, optional
        分片大小（MB），仅在use_chunked=True时有效
    """
    
    # 如果没有指定输出文件名，则生成默认输出文件名
    if output_file is None:
        directory = os.path.dirname(input_file)
        filename = os.path.basename(input_file)
        base_name = filename.split('.')[0]
        output_file = os.path.join(directory, f"{base_name}_8bit.v3draw")

    return convert_16bit_to_8bit(
        input_file,
        output_file,
        scaling_method=scaling_method,
        use_chunked=use_chunked,
        chunk_size_mb=chunk_size_mb
    )


if __name__ == "__main__":
    # python3 {script_path} --image-path "{image_path}"
    # 解析参数 --image-path
    import argparse
    parser = argparse.ArgumentParser(description="Convert 16-bit v3d images to 8-bit.")
    parser.add_argument('--image-path', type=str, required=True, help='Path to the v3d image file or directory.')
    args = parser.parse_args()

    input_path = args.image_path
    input_path = input_path.strip()
    if os.path.isdir(input_path):
        # 批量处理目录下的所有v3d文件
        Batch_Convert16bitTo8bit(input_path)
    else:
        # 单个文件处理
        Single_Convert16bitTo8bit(input_path)