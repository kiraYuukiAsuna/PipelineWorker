import config as cfg
import os
import pandas as pd
import numpy as np
from v3dpy.loaders import Raw, PBD

import os
import pandas as pd
import numpy as np


class DataAccess:
    def __init__(self, soma_dir, img_dir):
        """
        :param soma_dir: soma标记文件路径顶层目录
        :param img_dir: 原始图像文件路径顶层目录
        """
        self.soma_dir = soma_dir
        self.img_dir = img_dir

    def get_soma_list(self, ptrsid, docid):
        """
        获取单个 document 的 soma 坐标信息
        :param ptrsid: Pxxxx-Txxx-Rxxx-Sxxx 等前缀 ID
        :param docid: 具体的 document_id (形如 P00125-T001-R002-S009-B1)
        :return: 包含 x, y, z 等信息的 DataFrame
        """
        apo_file = os.path.join(self.soma_dir, ptrsid,
                                docid, docid + '_initial.apo')
        if not os.path.exists(apo_file):
            print(f"Can not find soma file: {apo_file}")
            return None

        somalist = pd.read_csv(apo_file)
        return somalist

    def get_image_data(self, ptrsid, docid, raw_loader):
        """
        获取单个 document 的三维数据
        :return: np.array 形状 (c, z, y, x)
        """
        imgpath = os.path.join(self.img_dir, ptrsid, docid + '_8bit.v3draw')
        if not os.path.exists(imgpath):
            print(f"Can not find image file: {imgpath}")
            return None

        img = raw_loader.load(imgpath)
        return img


def fp2dbdirs(strcid):
    '''convert file path to Database dirs'''
    # fname = os.path.split(file_path)[-1]
    # strcid = fname.split(".")[0]
    cid = int(strcid)
    # first layer of dir : 1000
    minb = int(cid / 1000) * 1000
    maxb = (int(cid / 1000) + 1) * 1000 - 1
    str_minb = ('00000' + str(minb))[-5:]
    str_maxb = ('00000' + str(maxb))[-5:]
    fdir = str_minb + '_' + str_maxb
    # second layer of dir : 100
    layer_base = int(cid / 1000) * 1000
    minb1 = layer_base + int((cid - layer_base) / 100) * 100
    maxb1 = layer_base + int((cid - layer_base) / 100 + 1) * 100 - 1
    str_minb1 = ('00000' + str(minb1))[-5:]
    str_maxb1 = ('00000' + str(maxb1))[-5:]
    fdir1 = str_minb1 + '_' + str_maxb1
    todir2 = os.path.join(fdir, fdir1)
    return todir2


def main(h5_image_name):
    # h5_image_name (Input Parm 1)
    # Soma Marker Files: Located in /PBS/BRAINTELL/Projects/HumanNeurons/AllBrainSlices/HNDB_files/SamplePreparation/{ptrsid}/{docid}/{docid}_initial.apo
    # Raw Image Files: Located in /PBS/BRAINTELL/Projects/HumanNeurons/AllBrainSlices/PTRSB_DB/{ptrsid}/{docid}_8bit.v3draw

    # Cell Id Range (Input Parm 2)
    # cell_id_begin = 1
    # cell_id_end = 71659

    # {
    #     "h5_image_name": "PTRSB_DB.pyramid.h5",
    #     "cell_id_begin": 1,
    #     "cell_id_end": 71659
    # }

    # Begin Process #
    raw_loader = Raw()
    cell_pbd_saver = PBD()

    # 胞体信息文件与图像文件根目录
    soma_dir = cfg.SamplePreparationDirectory
    img_dir = cfg.PTRSB_DBDirectory

    # 图像的裁剪结果保存目录
    topbd = cfg.Cell_ImagesDirectory

    # 初始化数据访问类
    data_accessor = DataAccess(soma_dir, img_dir)

    xy_size = 700  # 裁剪图像边长的一半

    if h5_image_name.endswith(".pyramid.h5"):
        docid = h5_image_name.replace(".pyramid.h5.pyramid.h5", "")
    else:
        docid = h5_image_name.replace(".h5.pyramid.h5", "")

    # 处理 docid，得到 ptrsid
    ptrsid = docid
    docid_split = docid.split('-')
    if len(docid_split) == 5:  # P-T-R-S-B
        # 如果末位不是 Bxxx，则删去最后一个段
        if not docid_split[-1].startswith('B'):
            ptrsid = docid[0:-1 * (len(docid_split[4]) + 1)]
    elif len(docid_split) == 6:  # P-T-R-S-B-N
        ptrsid = docid[0:-1 * (len(docid_split[5]) + 1)]

    # 读取 CSV 文件获取 Cell ID 范围
    csv_file_path = os.path.join(
        cfg.SamplePreparationDirectory, ptrsid, docid, "cell_with_cellID.csv")
    if csv_file_path and os.path.exists(csv_file_path):
        df = pd.read_csv(csv_file_path, encoding='UTF-8')
        cell_id_begin = int(df['Cell ID'].min())
        cell_id_end = int(df['Cell ID'].max())
        print(f"从CSV文件读取到的Cell ID范围: {cell_id_begin} - {cell_id_end}")
    else:
        raise Exception("未找到CSV文件")

    # 获取该 docid 最小的 cell_id，后续依次递增
    this_sid = cell_id_begin

    # 加载 soma 坐标
    somalist = data_accessor.get_soma_list(ptrsid, docid)
    if somalist is None:
        print("Soma info not found for:.pyramid.h5", docid)
        raise Exception("Soma info not found for: " + docid)

    print('Now:', docid, 'Somas=', somalist.shape[0])

    # 加载原始图像数据 (c, z, y, x)
    img = data_accessor.get_image_data(ptrsid, docid, raw_loader)
    if img is None:
        print("Image data not found for:.pyramid.h5", docid)
        raise Exception("Image data not found for: " + docid)

    img_c, img_z, img_y, img_x = img.shape[0], img.shape[1], img.shape[2], img.shape[3]
    print(img_c, img_x, img_y, img_z)

    # 对每个 soma 坐标进行裁剪操作
    for s in somalist.index:
        sx = int(float(somalist.loc[s, 'x']))
        sy = int(float(somalist.loc[s, 'y']))
        sz = int(float(somalist.loc[s, 'z']))

        x_start = max(sx - xy_size, 0)
        x_end = min(sx + xy_size + 1, img_x)
        y_start = max(sy - xy_size, 0)
        y_end = min(sy + xy_size + 1, img_y)

        if x_start >= x_end or y_start >= y_end:
            print("Invalid crop region for cell_id:.pyramid.h5",
                  this_sid, "at x,y =.pyramid.h5", sx, sy)
            continue

        # 新的 soma 相对于裁剪子图的坐标
        newsx = sx - x_start
        newsy = sy - y_start

        # 保存结果的目录
        outsdir = os.path.join(topbd, fp2dbdirs(this_sid))
        if not os.path.exists(outsdir):
            print('Create directory:', outsdir)
            try:
                os.makedirs(outsdir, exist_ok=True)
                print(f'Successfully created directory: {outsdir}')
            except PermissionError:
                print(f'Permission denied: Cannot create directory {outsdir}')
                print('Please check if you have write permissions to this path')
                continue
            except Exception as e:
                print(f'Error creating directory {outsdir}: {e}')
                continue

        out_file = os.path.join(outsdir, str(this_sid) + '.v3dpbd')
        if not os.path.exists(out_file):
            print('Save:', str(this_sid) + '.v3dpbd')
            # 按 (c, z, y, x) 进行裁剪
            cropped_img = img[:, :, y_start:y_end, x_start:x_end]
            # 保存成 .v3dpbd
            cell_pbd_saver.save(out_file, np.array(cropped_img))

            # 在cfg.Cell_MIPsDirectory下保存 MIP 图像
            out_mip_dir = os.path.join(
                cfg.Cell_MIPsDirectory, fp2dbdirs(this_sid))
            if not os.path.exists(out_mip_dir):
                try:
                    os.makedirs(out_mip_dir, exist_ok=True)
                    print(f'Successfully created MIP directory: {out_mip_dir}')
                except PermissionError:
                    print(
                        f'Permission denied: Cannot create MIP directory {out_mip_dir}')
                    print('Skipping MIP generation for this cell')
                except Exception as e:
                    print(f'Error creating MIP directory {out_mip_dir}: {e}')
                    print('Skipping MIP generation for this cell')
                else:
                    # 只有在成功创建目录后才生成 MIP
                    mip_file = os.path.join(
                        out_mip_dir, str(this_sid) + '.tif')
                    if not os.path.exists(mip_file):
                        # 计算 MIP 图像 Z 方向的最大投影
                        mip_img = np.max(cropped_img, axis=1)  # 在 Z 轴上进行最大投影
                        # 保存 MIP 图像
                        from skimage import io
                        io.imsave(mip_file, mip_img.astype(np.uint8))
            else:
                # 目录已存在，直接生成 MIP
                mip_file = os.path.join(out_mip_dir, str(this_sid) + '.tif')
                if not os.path.exists(mip_file):
                    # 计算 MIP 图像 Z 方向的最大投影
                    mip_img = np.max(cropped_img, axis=1)  # 在 Z 轴上进行最大投影
                    # 保存 MIP 图像
                    from skimage import io
                    io.imsave(mip_file, mip_img.astype(np.uint8))

        # 递增 cell_id
        this_sid += 1

        # 检查保存是否成功
        if not os.path.exists(out_file):
            print('Fail to generate ', str(this_sid) + '.v3dpbd')


if __name__ == '__main__':
    images = [
        "P00136-T001-R001-S028-B1.pyramid.h5",
        "P00136-T001-R001-S027-B1.pyramid.h5",
        "P00136-T001-R001-S026-B1.pyramid.h5",
        "P00136-T001-R001-S024-B1.pyramid.h5",
        "P00136-T001-R001-S023-B1.pyramid.h5",
        "P00136-T001-R001-S021-B1.pyramid.h5",
        "P00136-T001-R001-S018-B1.pyramid.h5",
        "P00133-T001-R001-S010-B1.pyramid.h5",
        "P00131-T001-R002-S021-B1.pyramid.h5",
        "P00131-T001-R002-S015-B1.pyramid.h5",
        "P00136-T001-R001-S025-B1.pyramid.h5",
        "P00131-T001-R002-S017-B1.pyramid.h5",
        "P00136-T001-R001-S020-B2.pyramid.h5",
        "P00136-T001-R001-S020-B1.pyramid.h5",
        "P00135-T001-R003-S026-B1.pyramid.h5",
        "P00135-T001-R003-S003-B1.pyramid.h5",
        "P00134-T001-R001-S008-B1.pyramid.h5",
        "P00134-T001-R001-S006-B1.pyramid.h5",
        "P00134-T001-R001-S004-B1.pyramid.h5",
        "P00133-T001-R001-S009-B1.pyramid.h5",
        "P00133-T001-R001-S007-B1.pyramid.h5",
        "P00133-T001-R001-S006-B1.pyramid.h5",
        "P00131-T001-R002-S009-B1.pyramid.h5",
        "P00133-T001-R002-S005-B1.pyramid.h5",
        "P00131-T001-R002-S014-B1.pyramid.h5",
        "P00131-T001-R002-S004-B1.pyramid.h5",
        "P00134-T001-R001-S013-B1.pyramid.h5",
        "P00134-T001-R001-S012-B1.pyramid.h5",
        "P00134-T001-R001-S011-B1.pyramid.h5",
        "P00134-T001-R001-S010-B1.pyramid.h5",
        "P00134-T001-R001-S009-B1.pyramid.h5",
        "P00134-T001-R001-S007-B1.pyramid.h5",
        "P00134-T001-R001-S005-B1.pyramid.h5",
        "P00131-T001-R004-S012-B1.pyramid.h5",
        "P00131-T001-R004-S011-B1.pyramid.h5",
        "P00131-T001-R003-S009-B1.pyramid.h5",
        "P00131-T001-R002-S008-B1.pyramid.h5",
        "P00131-T001-R002-S002-B1.pyramid.h5",
        "P00132-T001-R003-S010-B1.pyramid.h5",
        "P00131-T001-R004-S019-B1.pyramid.h5",
        "P00131-T001-R004-S017-B1.pyramid.h5",
        "P00131-T001-R003-S011-B1.pyramid.h5",
        "P00133-T001-R002-S009-B1.pyramid.h5",
        "P00131-T001-R004-S022-B1.pyramid.h5",
        "P00131-T001-R002-S003-B1.pyramid.h5",
        "P00130-T001-R001-S028-B1.pyramid.h5",
        "P00131-T001-R003-S012-B1.pyramid.h5",
        "P00131-T001-R002-S018-B1.pyramid.h5",
        "P00131-T001-R002-S011-B1.pyramid.h5",
        "P00131-T001-R002-S006-B1.pyramid.h5",
        "P00131-T001-R001-S007-B1.pyramid.h5",
        "P00128-T001-R001-S005-B1.pyramid.h5",
        "P00131-T001-R001-S004-B1.pyramid.h5",
        "P00131-T001-R001-S003-B1.pyramid.h5",
        "P00131-T001-R001-S002-B1.pyramid.h5",
    ]

    images.reverse()
    for h5_image_name in images:
        print(f"Processing: {h5_image_name}")
        try:
            main(h5_image_name)
        except Exception as e:
            print(f"Error processing {h5_image_name}: {e}")

    print("All images processed.")