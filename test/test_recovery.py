#!/usr/bin/env python3
"""
Pipeline Worker 重启恢复测试

这个脚本展示了 Pipeline Worker 如何在重启后恢复监测之前的 slurm 任务
"""

import json
import os
import sys
from datetime import datetime

def simulate_running_jobs():
    """模拟一些运行中的任务，用于测试重启恢复功能"""
    
    # 模拟的运行任务数据
    running_jobs_data = {
        "pipeline1_mip_generation": {
            "job_id": "12345",
            "pipeline_id": "pipeline1", 
            "step_name": "mip_generation",
            "submit_time": "2025-08-08T10:00:00",
            "last_check_time": "2025-08-08T10:30:00",
            "h5_image_name": "test_image.pyramid.h5",
            "status": "RUNNING"
        },
        "pipeline2_bit_conversion": {
            "job_id": "12346",
            "pipeline_id": "pipeline2",
            "step_name": "bit_conversion", 
            "submit_time": "2025-08-08T10:15:00",
            "last_check_time": "2025-08-08T10:30:00",
            "h5_image_name": "another_image.pyramid.h5",
            "status": "PENDING"
        }
    }
    
    # 保存到文件
    with open("running_jobs.json", "w", encoding="utf-8") as f:
        json.dump(running_jobs_data, f, ensure_ascii=False, indent=2)
    
    print("✅ 模拟运行任务数据已保存到 running_jobs.json")
    print("📋 当前模拟的运行任务:")
    for job_key, job_data in running_jobs_data.items():
        print(f"  - {job_data['job_id']}: {job_data['pipeline_id']}/{job_data['step_name']} [{job_data['status']}]")

def show_recovery_demo():
    """展示恢复功能的说明"""
    print("""
🔄 Pipeline Worker 重启恢复功能

新功能特性:
1. ✅ 任务持久化: 所有运行中的 slurm 任务信息自动保存到 running_jobs.json 文件
2. ✅ 自动恢复: 重启后自动从文件恢复之前的任务状态
3. ✅ 状态验证: 启动时验证恢复的任务是否仍在运行
4. ✅ 进度继续: 继续监控和汇报恢复任务的进度

工作流程:
1. 任务提交时 → 保存到 running_jobs.json
2. 状态变化时 → 更新 running_jobs.json
3. 重启程序时 → 从 running_jobs.json 恢复任务
4. 恢复后验证 → 检查任务是否仍在运行
5. 继续监控 → 正常监控和汇报进度

使用方法:
1. 正常启动 Pipeline Worker
2. 提交一些任务
3. 重启程序
4. 查看日志确认任务已恢复

注意事项:
- 只有运行中的任务会被持久化和恢复
- 已完成的任务会从恢复列表中移除
- 无法连接的任务会标记为失败状态
""")

def check_recovery_file():
    """检查恢复文件状态"""
    if os.path.exists("running_jobs.json"):
        print("📁 发现现有的运行任务文件:")
        with open("running_jobs.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        
        print(f"   包含 {len(data)} 个任务:")
        for job_key, job_data in data.items():
            print(f"   - {job_data['job_id']}: {job_data['pipeline_id']}/{job_data['step_name']} [{job_data['status']}]")
    else:
        print("📭 未找到运行任务文件 (running_jobs.json)")

if __name__ == "__main__":
    print("🚀 Pipeline Worker 重启恢复测试工具\n")
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "simulate":
            simulate_running_jobs()
        elif command == "check":
            check_recovery_file()
        elif command == "demo":
            show_recovery_demo()
        elif command == "clean":
            if os.path.exists("running_jobs.json"):
                os.remove("running_jobs.json")
                print("🗑️  已清理 running_jobs.json 文件")
            else:
                print("📭 running_jobs.json 文件不存在")
        else:
            print("❌ 未知命令")
    else:
        print("使用方法:")
        print("  python test_recovery.py simulate  # 模拟运行任务")
        print("  python test_recovery.py check     # 检查恢复文件")
        print("  python test_recovery.py demo      # 显示功能说明")
        print("  python test_recovery.py clean     # 清理恢复文件")
        print()
        check_recovery_file()
