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

    # 正则表达式匹配图像ID模式: P00134-T001-R001-S012-B1-N1
    pattern = r'(P\d+-T\d+-R\d+-S\d+(?:-B\d+)?(?:-N\d+)?)'

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
                # image id P095-T01-R01-S004-B1
                # ptrs_id P095-T01-R01-S004 (remove -B1)
                # 处理 docid，得到 ptrsid
                docid = image_id
                ptrsid = docid
                docid_split = docid.split('-')
                if len(docid_split) == 5: # P-T-R-S-B
                    # 如果末位不是 Bxxx，则删去最后一个段
                    if not docid_split[-1].startswith('B'):
                        ptrsid = docid[0:-1 * (len(docid_split[4]) + 1)]
                elif len(docid_split) == 6: # P-T-R-S-B-N
                    ptrsid = docid[0:-1 * (len(docid_split[5]) + 1)]

                if filename.endswith("_MIP.tif"):
                    # 额外保存到样本目录
                    sample_target_dir = os.path.join(cfg.SamplePreparationDirectory, ptrsid, image_id)
                    os.makedirs(sample_target_dir, exist_ok=True)
                    sample_target_path = os.path.join(sample_target_dir, filename)
                    if not os.path.exists(sample_target_path):
                        print(f"  复制: {filename} 到样本目录")
                        shutil.copy(source_path, sample_target_path)

                if is_valid_type:
                    print(f"  移动: {filename}")
                    shutil.move(source_path, target_path)
            except Exception as e:
                print(f"  移动文件 {filename} 时出错: {e}")

    print("文件整理完成!")

def ArchiveCommand():
    print("执行归档命令...")

    for dirpath, dirnames, filenames in os.walk(cfg.ImageProcessedFilesArchive):
        for dirname in dirnames:
            shutil.move(
                os.path.join(dirpath, dirname),
                cfg.ImageArchiveDirectory
            )

    print("归档命令执行完成!")

if __name__ == "__main__":
    organize_image_files()
    ArchiveCommand()
