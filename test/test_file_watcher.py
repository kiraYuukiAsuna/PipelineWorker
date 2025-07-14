#!/usr/bin/env python3
"""
测试UploadFileWatcher功能的简单脚本
"""

import os
import time
import tempfile
import shutil
from pathlib import Path

def test_upload_file_watcher():
    """测试文件监控功能"""
    print("开始测试UploadFileWatcher功能...")
    
    # 创建临时测试目录
    test_dir = tempfile.mkdtemp(prefix="h5_test_")
    print(f"测试目录: {test_dir}")
    
    try:
        # 模拟配置
        import sys
        sys.path.append('.')
        
        # 创建一个模拟的config模块
        with open('test_config.py', 'w') as f:
            f.write(f'ImageRootDirectory = "{test_dir}"\n')
        
        # 导入并修改配置
        import config as cfg
        
        # 模拟创建.h5文件
        test_h5_file = os.path.join(test_dir, "test_image.h5")
        
        print("创建测试.h5文件...")
        with open(test_h5_file, 'wb') as f:
            # 写入一些模拟数据
            f.write(b"mock h5 data" * 1000)  # 模拟较大文件
        
        print(f"测试文件已创建: {test_h5_file}")
        print(f"文件大小: {os.path.getsize(test_h5_file)} bytes")
        
        # 这里在实际环境中会触发UploadFileWatcher
        print("在实际环境中，UploadFileWatcher会检测到这个文件并处理")
        
    finally:
        # 清理测试文件
        shutil.rmtree(test_dir, ignore_errors=True)
        if os.path.exists('test_config.py'):
            os.remove('test_config.py')
        print("测试完成，清理临时文件")

if __name__ == "__main__":
    test_upload_file_watcher()
