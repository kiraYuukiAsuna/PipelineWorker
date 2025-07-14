#!/usr/bin/env python3
"""
MIP图像方向修正工具

提供多种图像方向修正选项：
- 上下翻转 (flipud)
- 左右翻转 (fliplr) 
- 90度旋转
- 180度旋转
- 270度旋转
- 转置 (transpose)

用户可以根据需要选择合适的修正方式。
"""

import numpy as np
import os
import sys
from pipeline_stage_h5_to_mip import transfer_h5_to_mip_chunked, get_shape_from_h5file, process_mip
from PIL import Image
import matplotlib.pyplot as plt


def apply_orientation_fix(image, fix_type):
    """
    应用不同类型的图像方向修正
    
    Parameters:
    -----------
    image : numpy.ndarray
        输入图像
    fix_type : str
        修正类型：
        - 'none': 不修正
        - 'flipud': 上下翻转
        - 'fliplr': 左右翻转  
        - 'both_flip': 上下+左右翻转
        - 'rot90': 顺时针90度旋转
        - 'rot180': 180度旋转
        - 'rot270': 顺时针270度旋转
        - 'transpose': 转置（行列互换）
        - 'transpose_flip': 转置+上下翻转
    
    Returns:
    --------
    numpy.ndarray: 修正后的图像
    """
    if fix_type == 'none':
        return image
    elif fix_type == 'flipud':
        return np.flipud(image)
    elif fix_type == 'fliplr':
        return np.fliplr(image)
    elif fix_type == 'both_flip':
        return np.fliplr(np.flipud(image))
    elif fix_type == 'rot90':
        return np.rot90(image, k=1)  # 顺时针90度
    elif fix_type == 'rot180':
        return np.rot90(image, k=2)  # 180度
    elif fix_type == 'rot270':
        return np.rot90(image, k=3)  # 顺时针270度
    elif fix_type == 'transpose':
        return image.T
    elif fix_type == 'transpose_flip':
        return np.flipud(image.T)
    else:
        raise ValueError(f"未知的修正类型: {fix_type}")


def generate_all_orientations(input_file, output_dir, chunk_size_z=50):
    """
    生成所有可能的图像方向版本，帮助用户找到正确的方向
    
    Parameters:
    -----------
    input_file : str
        输入H5文件路径
    output_dir : str
        输出目录
    chunk_size_z : int
        Z轴分片大小
    """
    if not os.path.exists(input_file):
        print(f"错误：输入文件不存在 - {input_file}")
        return False
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    
    # 所有可能的修正类型
    fix_types = [
        ('none', '原始（无修正）'),
        ('flipud', '上下翻转'),
        ('fliplr', '左右翻转'),
        ('both_flip', '上下+左右翻转'),
        ('rot90', '顺时针90度旋转'),
        ('rot180', '180度旋转'),
        ('rot270', '顺时针270度旋转'),
        ('transpose', '转置'),
        ('transpose_flip', '转置+上下翻转')
    ]
    
    print(f"开始处理文件: {input_file}")
    print(f"输出目录: {output_dir}")
    print(f"将生成 {len(fix_types)} 个不同方向的MIP图像")
    print("=" * 60)
    
    # 首先计算原始MIP（只计算一次，然后应用不同的变换）
    print("计算原始MIP...")
    try:
        shapes, arrangements = get_shape_from_h5file(input_file)
        cur_level = str(max([int(key) for key in shapes]))
        
        # 使用修改过的函数，不应用任何修正
        image_mip = None
        
        # 这里我们需要重新实现MIP计算，不使用process_mip的修正功能
        from pipeline_stage_h5_to_mip import compute_mip_chunked, get_h5_dataset_info
        import h5py
        
        if cur_level != '0':
            cols, rows = arrangements[cur_level]
            total_datasets = rows * cols
            
            # 获取所有数据集的信息
            datasets_info = []
            for i in range(total_datasets):
                info = get_h5_dataset_info(input_file, cur_level, i)
                datasets_info.append(info)
            
            # 计算分片MIP
            image_mip = compute_mip_chunked(input_file, cur_level, datasets_info, arrangements, chunk_size_z)
        else:
            # 处理单一数据集
            with h5py.File(input_file, 'r') as f:
                image = f['0']['0']['0'][:]
                if len(image.shape) == 4:
                    image = image[:, :, :, 0]
                image_mip = np.max(image, axis=0)
        
        if image_mip is None:
            print("错误：无法计算MIP")
            return False
        
        print(f"原始MIP形状: {image_mip.shape}")
        
        # 基础处理（去噪、归一化等，但不修正方向）
        from skimage import exposure, filters
        mip_denoised = filters.median(image_mip)
        mip_normalized = (mip_denoised - np.min(mip_denoised)) / \
            (np.max(mip_denoised) - np.min(mip_denoised))
        mip_adapteq = exposure.equalize_adapthist(mip_normalized, clip_limit=0.01)
        mip_base = (mip_adapteq * 255).astype(np.uint8)
        
        # 为每种修正类型生成图像
        success_count = 0
        for fix_type, description in fix_types:
            output_file = os.path.join(output_dir, f"{base_name}_mip_{fix_type}.tiff")
            
            print(f"生成: {description} -> {os.path.basename(output_file)}")
            
            try:
                # 应用方向修正
                mip_corrected = apply_orientation_fix(mip_base, fix_type)
                
                # 保存图像
                Image.fromarray(mip_corrected).save(output_file, format='TIFF')
                print(f"  ✓ 成功保存: {output_file}")
                success_count += 1
                
            except Exception as e:
                print(f"  ✗ 生成失败: {e}")
        
        print("\n" + "=" * 60)
        print(f"完成！成功生成 {success_count}/{len(fix_types)} 个图像")
        print(f"输出目录: {output_dir}")
        print("\n建议:")
        print("1. 使用图像查看器打开输出目录中的所有TIFF文件")
        print("2. 比较不同版本，找到方向最正确的版本")
        print("3. 记住正确版本的修正类型，在后续处理中使用")
        
        return success_count > 0
        
    except Exception as e:
        print(f"处理失败: {e}")
        return False


def create_comparison_grid(input_dir, output_file="mip_comparison.png"):
    """
    创建所有MIP版本的对比网格图
    
    Parameters:
    -----------
    input_dir : str
        包含所有MIP图像的目录
    output_file : str
        输出对比图的文件名
    """
    try:
        import matplotlib.pyplot as plt
        from PIL import Image
        import glob
        
        # 查找所有MIP图像
        mip_files = glob.glob(os.path.join(input_dir, "*_mip_*.tiff"))
        if not mip_files:
            print("未找到MIP图像文件")
            return False
        
        # 按文件名排序
        mip_files.sort()
        
        # 计算网格大小
        n_images = len(mip_files)
        cols = 3
        rows = (n_images + cols - 1) // cols
        
        fig, axes = plt.subplots(rows, cols, figsize=(15, rows * 5))
        fig.suptitle('MIP图像方向对比', fontsize=16)
        
        if rows == 1:
            axes = [axes]
        
        for i, mip_file in enumerate(mip_files):
            row = i // cols
            col = i % cols
            
            # 读取图像
            img = np.array(Image.open(mip_file))
            
            # 获取修正类型
            fix_type = os.path.basename(mip_file).split('_mip_')[-1].replace('.tiff', '')
            
            if rows > 1:
                ax = axes[row][col]
            else:
                ax = axes[col]
            
            ax.imshow(img, cmap='gray')
            ax.set_title(f'{fix_type}', fontsize=12)
            ax.axis('off')
        
        # 隐藏多余的子图
        for i in range(n_images, rows * cols):
            row = i // cols
            col = i % cols
            if rows > 1:
                axes[row][col].axis('off')
            else:
                axes[col].axis('off')
        
        plt.tight_layout()
        output_path = os.path.join(input_dir, output_file)
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        print(f"对比网格图已保存: {output_path}")
        return True
        
    except ImportError:
        print("需要matplotlib库来生成对比图")
        return False
    except Exception as e:
        print(f"生成对比图失败: {e}")
        return False


def main():
    """主函数"""
    if len(sys.argv) < 3:
        print("用法: python mip_orientation_tool.py <h5_file_path> <output_dir>")
        print("示例: python mip_orientation_tool.py /path/to/file.h5 /path/to/output")
        print("\n此工具将生成多个不同方向的MIP图像，帮助您找到正确的方向。")
        return
    
    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    
    print("MIP图像方向修正工具")
    print("=" * 60)
    
    # 生成所有方向版本
    success = generate_all_orientations(input_file, output_dir)
    
    if success:
        # 生成对比网格图
        print("\n生成对比网格图...")
        create_comparison_grid(output_dir)


if __name__ == "__main__":
    main()
