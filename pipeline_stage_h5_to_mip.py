import numpy as np
import h5py
import os
from functools import reduce
import matplotlib.pyplot as plt
from PIL import Image
from skimage import exposure, filters, morphology
import gc
import psutil



def get_memory_usage():
    """获取当前内存使用情况"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # MB


def process_mip(mip, orientation_fix='both_flip'):
    """
    处理MIP图像：去噪、归一化、直方图均衡化，修正方向
    
    Parameters:
    -----------
    mip : numpy.ndarray
        输入MIP图像
    orientation_fix : str
        方向修正类型：
        - 'none': 不修正
        - 'flipud': 上下翻转
        - 'fliplr': 左右翻转  
        - 'both_flip': 上下+左右翻转（默认）
        - 'rot90': 顺时针90度旋转
        - 'rot180': 180度旋转
        - 'rot270': 顺时针270度旋转
        - 'transpose': 转置
        - 'transpose_flip': 转置+上下翻转
    """
    # 中值滤波去噪
    mip_denoised = filters.median(mip)
    # 归一化到0-1
    mip_normalized = (mip_denoised - np.min(mip_denoised)) / \
        (np.max(mip_denoised) - np.min(mip_denoised))
    # 自适应直方图均衡化
    mip_adapteq = exposure.equalize_adapthist(
        mip_normalized, clip_limit=0.01)
    # 映射到0-255并转换为uint8
    mip_uint8 = (mip_adapteq * 255).astype(np.uint8)
    
    # 应用方向修正
    if orientation_fix == 'none':
        pass  # 不修正
    elif orientation_fix == 'flipud':
        mip_uint8 = np.flipud(mip_uint8)
    elif orientation_fix == 'fliplr':
        mip_uint8 = np.fliplr(mip_uint8)
    elif orientation_fix == 'both_flip':
        mip_uint8 = np.flipud(mip_uint8)
        mip_uint8 = np.fliplr(mip_uint8)
    elif orientation_fix == 'rot90':
        mip_uint8 = np.rot90(mip_uint8, k=1)
    elif orientation_fix == 'rot180':
        mip_uint8 = np.rot90(mip_uint8, k=2)
    elif orientation_fix == 'rot270':
        mip_uint8 = np.rot90(mip_uint8, k=3)
    elif orientation_fix == 'transpose':
        mip_uint8 = mip_uint8.T
    elif orientation_fix == 'transpose_flip':
        mip_uint8 = np.flipud(mip_uint8.T)
    else:
        print(f"警告：未知的方向修正类型 '{orientation_fix}'，使用默认设置")
        mip_uint8 = np.flipud(mip_uint8)
        mip_uint8 = np.fliplr(mip_uint8)
    
    return mip_uint8


def get_h5_dataset_info(input_file, level, index):
    """获取H5数据集的基本信息而不加载数据"""
    with h5py.File(input_file, 'r') as f:
        dataset = f['0'][str(level)][str(index)]  # type: ignore
        return {
            'shape': dataset.shape,  # type: ignore
            'dtype': dataset.dtype,  # type: ignore
            'size_mb': np.prod(dataset.shape) * dataset.dtype.itemsize / (1024 * 1024)  # type: ignore
        }


def load_h5_chunk(input_file, level, index, slice_range=None):
    """加载H5数据集的指定片段"""
    with h5py.File(input_file, 'r') as f:
        dataset = f['0'][str(level)][str(index)]  # type: ignore
        if slice_range is None:
            return dataset[:]  # type: ignore
        else:
            start_z, end_z = slice_range
            return dataset[start_z:end_z, :, :]  # type: ignore


def compute_mip_chunked(input_file, level, datasets_info, arrangements, chunk_size_z=50):
    """
    使用分片模式计算MIP，降低内存占用
    
    Parameters:
    -----------
    input_file : str
        H5文件路径
    level : str
        处理的层级
    datasets_info : list
        数据集信息列表
    arrangements : dict
        排列信息
    chunk_size_z : int
        Z轴分片大小
    
    Returns:
    --------
    numpy.ndarray: MIP图像
    """
    cols, rows = arrangements[level]
    total_datasets = rows * cols
    
    # 获取拼接后的总体尺寸
    tmp_info = datasets_info[0]
    total_z = tmp_info['shape'][0]
    
    print(f"开始分片MIP计算，Z轴总深度: {total_z}, 分片大小: {chunk_size_z}")
    print(f"预估内存使用: ~{(chunk_size_z * tmp_info['shape'][1] * tmp_info['shape'][2] * total_datasets * 4) / (1024*1024):.0f}MB per chunk")
    
    # 初始化MIP结果（将在第一个分片处理时确定最终尺寸）
    mip_result = None
    
    # 分片处理
    for z_start in range(0, total_z, chunk_size_z):
        z_end = min(z_start + chunk_size_z, total_z)
        current_chunk_size = z_end - z_start
        
        print(f"\r处理Z切片 {z_start}-{z_end-1} ({current_chunk_size}层)", end='', flush=True)
        
        # 获取拼接方法
        concat_method = get_concat_method(rows, cols, [info['shape'] for info in datasets_info])
          # 分片加载和拼接当前Z范围的数据
        chunk_datasets = []
        for i in range(total_datasets):
            chunk_data = load_h5_chunk(input_file, level, i, (z_start, z_end))
            
            # 处理多通道数据
            if len(chunk_data.shape) == 4:  # type: ignore
                chunk_data = chunk_data[:, :, :, 0]  # type: ignore
            
            chunk_datasets.append(chunk_data)
        
        # 拼接当前分片
        if concat_method == 1:
            # 先拼列
            imageL = []
            for i in range(cols):
                col_data = []
                for j in range(i, total_datasets, cols):
                    col_data.append(chunk_datasets[j])
                imageL.append(np.concatenate(col_data, axis=1))
            
            chunk_image = imageL[0]
            for img in imageL[1:]:
                chunk_image = np.concatenate([chunk_image, img], axis=2)
                
        elif concat_method == 2:
            # 先拼行
            imageL = []
            for i in range(rows):
                row_data = []
                for j in range(i*cols, i*cols+cols):
                    row_data.append(chunk_datasets[j])
                imageL.append(np.concatenate(row_data, axis=2))
            
            chunk_image = imageL[0]
            for img in imageL[1:]:
                chunk_image = np.concatenate([chunk_image, img], axis=1)
        else:
            raise Exception("Invalid concat method")
        
        # 释放临时数据
        del chunk_datasets, imageL
        gc.collect()
        
        # 计算当前分片的MIP
        chunk_mip = np.max(chunk_image, axis=0)
        del chunk_image
        gc.collect()
        
        # 更新全局MIP
        if mip_result is None:
            mip_result = chunk_mip.copy()
        else:
            mip_result = np.maximum(mip_result, chunk_mip)
        
        del chunk_mip
        gc.collect()
        
        # 显示内存使用情况
        current_memory = get_memory_usage()
        if z_start % (chunk_size_z * 10) == 0:  # 每10个分片显示一次
            print(f" [内存: {current_memory:.0f}MB]", end='')    
    if mip_result is not None:
        print(f"\n分片MIP计算完成，最终MIP尺寸: {mip_result.shape}")
    return mip_result


def transfer_h5_to_mip_chunked(input_file, output_path, chunk_size_z=50, use_chunked=True, orientation_fix='both_flip'):
    """
    使用分片模式处理H5文件生成MIP图像
    
    Parameters:
    -----------
    input_file : str
        输入H5文件路径
    output_path : str
        输出图像路径
    chunk_size_z : int
        Z轴分片大小，默认50层
    use_chunked : bool
        是否使用分片模式
    orientation_fix : str
        方向修正类型，可选：'none', 'flipud', 'fliplr', 'both_flip', 'rot90', 'rot180', 'rot270', 'transpose', 'transpose_flip'
    """
    print(f"开始处理文件: {input_file}")
    print(f"分片模式: {'启用' if use_chunked else '禁用'}")
    if use_chunked:
        print(f"Z轴分片大小: {chunk_size_z}")
    
    initial_memory = get_memory_usage()
    print(f"初始内存使用: {initial_memory:.0f}MB")
    
    try:
        shapes, arrangements = get_shape_from_h5file(input_file)
        cur_level = str(max([int(key) for key in shapes]))
        
        print(f"选择处理层级: {cur_level}")
        print(f"该层级图像尺寸: {shapes[cur_level]}")
        
        if use_chunked and cur_level != '0':
            # 使用分片模式
            cols, rows = arrangements[cur_level]
            total_datasets = rows * cols
            
            # 获取所有数据集的信息
            datasets_info = []
            total_size_mb = 0
            for i in range(total_datasets):
                info = get_h5_dataset_info(input_file, cur_level, i)
                datasets_info.append(info)
                total_size_mb += info['size_mb']
            
            print(f"总数据集数量: {total_datasets}")
            print(f"总数据大小: {total_size_mb:.0f}MB")
            
            # 计算分片MIP
            image_mip = compute_mip_chunked(input_file, cur_level, datasets_info, arrangements, chunk_size_z)
            
        else:            # 使用原始模式
            print("使用原始模式处理...")
            image = None
            with h5py.File(input_file, 'r') as f:
                if cur_level == '0':
                    image = f['0']['0']['0'][:]  # type: ignore
                elif cur_level in shapes:
                    cols, rows = arrangements[cur_level]
                    tmp_image_shapes = [
                        f['0'][str(cur_level)][str(j)].shape for j in range(rows*cols)]  # type: ignore
                    concat_method = get_concat_method(rows, cols, tmp_image_shapes)
                    imageL = []
                    if concat_method == 1:
                        # 先拼列
                        for i in range(cols):
                            imageL.append(np.concatenate([f['0'][str(cur_level)][str(j)][:] for j in range(i, i + rows*cols, cols)],  # type: ignore
                                                         axis=1))
                        image = imageL[0]
                        del imageL[0]
                        while imageL:
                            image = np.concatenate([image, imageL[0]], axis=2)
                            del imageL[0]
                    elif concat_method == 2:
                        for i in range(rows):
                            imageL.append(np.concatenate(
                                [f['0'][cur_level][str(j)][:] for j in range(i*cols, i*cols+cols)], axis=2))  # type: ignore
                        image = imageL[0]
                        del imageL[0]
                        while imageL:
                            image = np.concatenate([image, imageL[0]], axis=1)
                            del imageL[0]
                    else:
                        raise Exception("something wrong in function get_concat_method")
            
            # 多通道图像处理
            if image is not None and len(image.shape) == 4:  # type: ignore
                print('multi-channel shape:', image.shape, end=' ')  # type: ignore
                image = image[:, :, :, 0]  # type: ignore
                print('to', image.shape)  # type: ignore
            if image is not None:
                print('processed shape', image.shape)  # type: ignore
            
            peak_memory = get_memory_usage()
            print(f"加载完成，当前内存使用: {peak_memory:.0f}MB")
            
            # 计算MIP
            print("计算MIP...")
            if image is not None:
                image_mip = np.max(image, axis=0)  # type: ignore
                del image
                gc.collect()
            else:
                raise Exception("Failed to load image data")        # 处理MIP图像
        print("处理MIP图像...")
        mip_uint8 = process_mip(image_mip, orientation_fix)
        del image_mip
        gc.collect()
        
        # 保存结果
        out_file = output_path
        
        if not os.path.exists(os.path.dirname(out_file)):
            os.makedirs(os.path.dirname(out_file))
        
        Image.fromarray(mip_uint8).save(out_file, format='TIFF')
        
        final_memory = get_memory_usage()
        print(f"处理完成！MIP已保存至: {out_file}")
        print(f"最终内存使用: {final_memory:.0f}MB")
        print(f"峰值内存增长: {final_memory - initial_memory:.0f}MB")
        
        return True
        
    except Exception as e:
        error_memory = get_memory_usage()
        print(f"处理失败: {e}")
        print(f"错误时内存使用: {error_memory:.0f}MB")
        return False


def transfer_h5_to_mip(input_file, output_path, use_chunked=True, chunk_size_z=50, orientation_fix='both_flip'):
    """
    处理H5文件生成MIP图像的包装函数
    
    Parameters:
    -----------
    input_file : str
        输入H5文件路径
    output_path : str
        输出图像路径
    use_chunked : bool
        是否使用分片模式（推荐用于大文件）
    chunk_size_z : int
        Z轴分片大小，仅在use_chunked=True时有效
    orientation_fix : str
        方向修正类型：'none', 'flipud', 'fliplr', 'both_flip', 'rot90', 'rot180', 'rot270', 'transpose', 'transpose_flip'
    """
    if use_chunked:
        return transfer_h5_to_mip_chunked(input_file, output_path, chunk_size_z, True, orientation_fix)
    else:
        return transfer_h5_to_mip_chunked(input_file, output_path, chunk_size_z, False, orientation_fix)


def get_shape_from_h5file(input_file: str):
    """
    获取H5文件中的形状和排列信息
    """
    with h5py.File(input_file, 'r') as f:
        base_shape = f['0']['0']['0'].shape  # type: ignore
        base_score = np.log(base_shape[1]/base_shape[2])
        shapes = {}
        arrangements = {}
        for level in f['0']:  # type: ignore
            size = len(f['0'][level])  # type: ignore
            # 乘法分解
            arranges = factor_pairs(size)
            min_score = 1e9
            arrange = (0, 0)
            shape = (0, 0, 0)  # 初始化shape变量
            for row, col in arranges:
                size0 = f['0'][level]['0'].shape[0]  # type: ignore
                size1 = sum(f['0'][level][str(i)].shape[1] for i in range(col))  # type: ignore
                size2 = sum(f['0'][level][str(i)].shape[2]  # type: ignore
                            for i in range(0, size, col))
                score = abs(np.log(size1/size2)-base_score)
                if score < min_score:
                    min_score = score
                    shape = (size0, size1, size2)
                    arrange = (row, col)
            shapes[level] = shape
            arrangements[level] = arrange
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


def get_concat_method(rows, cols, image_shapes):
    # 如果返回1 先拼的每列的每个小块的第2个维度应该相等
    # 如果返回2 先拼的每行的每个小块的第1个维度应该相等
    tag = reduce(lambda x, y: x*y, [len(set([image_shapes[i][2]
                 for i in range(j, j+rows*cols, cols)])) for j in range(cols)]) == 1
    if tag:
        return 1
    else:
        tag = reduce(lambda x, y: x*y, [len(set([image_shapes[i][1]
                     for i in range(j*cols, j*cols+cols)])) for j in range(rows)]) == 1
        if tag:
            return 2
    return 0


def batch_process_h5_to_mip(input_dir, 
                           use_chunked=True, 
                           chunk_size_z=50,
                           orientation_fix='flipud'):
    """
    批量处理H5文件生成MIP
    
    Parameters:
    -----------
    input_dir : str
        输入目录路径
    use_chunked : bool
        是否使用分片模式
    chunk_size_z : int
        Z轴分片大小
    orientation_fix : str
        方向修正类型：'none', 'flipud', 'fliplr', 'both_flip', 'rot90', 'rot180', 'rot270', 'transpose', 'transpose_flip'
    """
    print(f"开始批量处理，输入目录: {input_dir}")
    print(f"处理模式: {'分片模式' if use_chunked else '原始模式'}")
    print(f"图像方向修正: {orientation_fix}")
    if use_chunked:
        print(f"分片大小: {chunk_size_z}")
    print("=" * 50)
    
    processed_count = 0
    success_count = 0
    
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.endswith('.h5'):
                file_path = os.path.join(root, file)
                processed_count += 1
                # 按照 . 分割文件名，取第一部分
                base_name = file.split('.')[0]
                # 拼接新的文件名
                output_filename = base_name + "_MIP.tif"
                output_path = os.path.join(root, output_filename)

                if os.path.exists(output_path):
                    print(f"{output_path} exist, skip")
                    continue

                print(f"\n[{processed_count}] 处理文件: {file}")
                try:
                    success = transfer_h5_to_mip(file_path, output_path, use_chunked, chunk_size_z, orientation_fix)
                    if success:
                        success_count += 1
                        print("✓ 处理成功")
                    else:
                        print("✗ 处理失败")
                except Exception as e:
                    print(f"✗ 处理异常: {e}")
                print("-" * 30)
    
    print(f"\n批量处理完成！")
    print(f"总文件数: {processed_count}")
    print(f"成功处理: {success_count}")
    print(f"失败数: {processed_count - success_count}")


def Single_H5ToMip(input_file, output_file=None, 
                   use_chunked=True, chunk_size_z=50, 
                   orientation_fix='flipud'):
    """
    单个文件H5转MIP入口函数

    Parameters:
    -----------
    input_file : str
        输入H5文件路径
    output_file : str, optional
        输出MIP图像文件路径
    use_chunked : bool, optional
        是否使用分片模式（推荐用于大文件）
    chunk_size_z : int, optional
        Z轴分片大小，仅在use_chunked=True时有效
    orientation_fix : str, optional
        方向修正类型：'none', 'flipud', 'fliplr', 'both_flip', 'rot90', 'rot180', 'rot270', 'transpose', 'transpose_flip'
    """

    # 如果没有指定输出文件名，则生成默认输出文件名
    if output_file is None:
        directory = os.path.dirname(input_file)
        filename = os.path.basename(input_file)
        base_name = filename.split('.')[0]
        output_file = os.path.join(directory, f"{base_name}_MIP.tif")

    return transfer_h5_to_mip(
        input_file,
        output_file,
        use_chunked=use_chunked,
        chunk_size_z=chunk_size_z,
        orientation_fix=orientation_fix
    )


if __name__ == "__main__":
    # python3 {script_path} --image-path "{image_path}"
    # 解析参数 --image-path
    import argparse
    parser = argparse.ArgumentParser(description="Process H5 files to generate MIP images.")
    parser.add_argument('--image-path', type=str, required=True, help='Path to the H5 file or directory.')
    args = parser.parse_args()

    input_path = args.image_path
    input_path = input_path.strip()
    if os.path.isdir(input_path):
        # 批量处理目录下的所有H5文件
        batch_process_h5_to_mip(
            input_path
        )
    else:
        # 单个文件处理
        Single_H5ToMip(
            input_path
        )
