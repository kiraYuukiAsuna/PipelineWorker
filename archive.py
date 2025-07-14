import os
import shutil
import re
import config as cfg

def organize_image_files():
    # 设置源目录和目标目录
    source_dir = cfg.ImageTransferTemp
    target_base_dir = cfg.ImageProcessedFilesArchive

    # 确保目标基础目录存在
    os.makedirs(target_base_dir, exist_ok=True)

    # 正则表达式匹配图像ID模式: P00134-T001-R001-S012-B1
    pattern = r'(P\d+-T\d+-R\d+-S\d+-B\d+)'

    # 获取源目录中的所有文件
    try:
        files = os.listdir(source_dir)
    except Exception as e:
        print(f"无法读取源目录: {e}")
        return

    # 按图像ID组织文件
    image_files = {}
    for filename in files:
        match = re.search(pattern, filename)
        if match:
            image_id = match.group(1)
            if image_id not in image_files:
                image_files[image_id] = []
            image_files[image_id].append(filename)

    # 处理每个图像ID
    for image_id, file_list in image_files.items():
        print(f"处理图像 {image_id}...")

        # 创建目标文件夹
        target_dir = os.path.join(target_base_dir, image_id)
        os.makedirs(target_dir, exist_ok=True)

        # 复制相关文件
        for filename in file_list:
            source_path = os.path.join(source_dir, filename)
            target_path = os.path.join(target_dir, filename)

            try:
                # 检查文件是否为所需的5种类型之一
                is_valid_type = any([
                    "_8bit_downsampled.v3draw" in filename,
                    "_8bit.v3draw" in filename or filename.endswith(".v3draw.8bit"),
                    "_MIP.tif" in filename,
                    ".pyramid.h5" in filename,
                    filename.endswith(".v3draw") and not "_" in filename.split(image_id)[1]
                ])

                if is_valid_type:
                    print(f"  复制: {filename}")
                    shutil.move(source_path, target_path)
            except Exception as e:
                print(f"  复制文件 {filename} 时出错: {e}")

    print("文件整理完成!")

def ArchiveCommand():
    print("执行归档命令...")
    shutil.move(
        cfg.ImageProcessedFilesArchive,
        cfg.ImageArchiveDirectory
    )
    print("归档命令执行完成!")

if __name__ == "__main__":
    organize_image_files()
    ArchiveCommand()
