"""
Pipeline Worker - 图像处理流程工作器
负责接收任务、提交sbatch作业、监控状态并与hndb api交互

新增功能:
- UploadFileWatcher: 监控cfg.ImageTransferTemp路径下的.h5文件上传
  * 实时监控指定目录下的.h5文件变化
  * 检测文件创建和移动事件
  * 等待文件上传完成（通过文件大小稳定性检查）
  * 自动通知core server有新文件上传
"""

import os
import sys
import time
import json
import asyncio
import subprocess
import logging
import httpx
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from dataclasses import dataclass
from enum import Enum as PyEnum
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
import uvicorn
import config as cfg
import archive

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_worker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StartStepRequest(BaseModel):
    """开始步骤请求模型"""
    pipeline_id: str
    step_name: str
    h5_image_name: str

class StepStatusEnum(str, PyEnum):
    """步骤状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

def map_slurm_status_to_step_status(slurm_status: str) -> str:
    """将 SLURM 状态映射到步骤状态"""
    slurm_to_step_mapping = {
        "PENDING": StepStatusEnum.PENDING.value,
        "RUNNING": StepStatusEnum.RUNNING.value,
        "COMPLETED": StepStatusEnum.COMPLETED.value,
        "FAILED": StepStatusEnum.FAILED.value,
        "CANCELLED": StepStatusEnum.FAILED.value,
        "TIMEOUT": StepStatusEnum.FAILED.value,
        "NODE_FAIL": StepStatusEnum.FAILED.value,
        "OUT_OF_MEMORY": StepStatusEnum.FAILED.value,
        "UNKNOWN": StepStatusEnum.FAILED.value,
        "ERROR": StepStatusEnum.FAILED.value
    }
    
    return slurm_to_step_mapping.get(slurm_status, StepStatusEnum.FAILED.value)

@dataclass
class JobInfo:
    """作业信息"""
    job_id: str
    pipeline_id: str
    step_name: str
    submit_time: datetime
    last_check_time: datetime
    h5_image_name: Optional[str] = None
    status: str = "PENDING"

class PipelineWorker:
    """Pipeline Worker 主类"""
    
    def __init__(self, 
                 core_server_url: str = "http://localhost:8000",
                 worker_id: Optional[str] = None,
                 heartbeat_interval: int = 30,
                 job_check_interval: int = 10):
        self.core_server_url = core_server_url.rstrip('/')
        self.worker_id = worker_id or f"worker_{os.getpid()}"
        self.heartbeat_interval = heartbeat_interval
        self.job_check_interval = job_check_interval
        
        # 存储正在运行的作业
        self.running_jobs: Dict[str, JobInfo] = {}
        
        # 持久化文件路径
        self.running_jobs_file = "running_jobs.json"

        # HTTP 客户端
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # 工作状态
        self.is_running = True
        
        # 后台任务
        self.background_tasks: List[asyncio.Task] = []
        
        # 启动时加载已运行的任务
        self._load_running_jobs()

        logger.info(f"Pipeline Worker 初始化完成，Worker ID: {self.worker_id}")

    def _save_running_jobs(self):
        """保存正在运行的任务到文件"""
        try:
            jobs_data = {}
            for job_key, job_info in self.running_jobs.items():
                jobs_data[job_key] = {
                    "job_id": job_info.job_id,
                    "pipeline_id": job_info.pipeline_id,
                    "step_name": job_info.step_name,
                    "submit_time": job_info.submit_time.isoformat(),
                    "last_check_time": job_info.last_check_time.isoformat(),
                    "h5_image_name": job_info.h5_image_name,
                    "status": job_info.status
                }

            with open(self.running_jobs_file, 'w', encoding='utf-8') as f:
                json.dump(jobs_data, f, ensure_ascii=False, indent=2)

            logger.debug(
                f"已保存 {len(jobs_data)} 个运行中的任务到 {self.running_jobs_file}")

        except Exception as e:
            logger.error(f"保存运行中任务失败: {e}")

    def _load_running_jobs(self):
        """从文件加载正在运行的任务"""
        try:
            if not os.path.exists(self.running_jobs_file):
                logger.info("未找到已保存的运行任务文件")
                return

            with open(self.running_jobs_file, 'r', encoding='utf-8') as f:
                jobs_data = json.load(f)

            for job_key, job_data in jobs_data.items():
                job_info = JobInfo(
                    job_id=job_data["job_id"],
                    pipeline_id=job_data["pipeline_id"],
                    step_name=job_data["step_name"],
                    submit_time=datetime.fromisoformat(
                        job_data["submit_time"]),
                    last_check_time=datetime.fromisoformat(
                        job_data["last_check_time"]),
                    h5_image_name=job_data.get("h5_image_name"),
                    status=job_data["status"]
                )
                self.running_jobs[job_key] = job_info

            logger.info(f"从文件恢复了 {len(jobs_data)} 个运行中的任务")

        except Exception as e:
            logger.error(f"加载运行中任务失败: {e}")

    async def _verify_recovered_jobs(self):
        """验证恢复的任务是否仍在运行"""
        try:
            # 等待一小段时间确保系统完全启动
            await asyncio.sleep(5)

            jobs_to_remove = []

            for job_key, job_info in list(self.running_jobs.items()):
                logger.info(
                    f"验证恢复的任务: {job_info.job_id} ({job_info.pipeline_id}/{job_info.step_name})")

                # 检查任务状态
                current_status = await self._check_job_status(job_info)

                if current_status in ["COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY"]:
                    logger.info(
                        f"恢复的任务已完成: {job_info.job_id} -> {current_status}")
                    await self._handle_job_completion(job_info, current_status)
                    jobs_to_remove.append(job_key)
                else:
                    logger.info(
                        f"恢复的任务仍在运行: {job_info.job_id} -> {current_status}")
                    job_info.status = current_status
                    job_info.last_check_time = datetime.now()

            # 移除已完成的任务
            for job_key in jobs_to_remove:
                del self.running_jobs[job_key]

            # 保存更新后的任务状态
            self._save_running_jobs()

        except Exception as e:
            logger.error(f"验证恢复的任务时发生错误: {e}")

    def _cleanup_running_jobs_file(self):
        """清理运行任务文件"""
        try:
            if os.path.exists(self.running_jobs_file):
                os.remove(self.running_jobs_file)
                logger.debug(f"已清理运行任务文件: {self.running_jobs_file}")
        except Exception as e:
            logger.error(f"清理运行任务文件失败: {e}")

    async def start_step(self, pipeline_id: str, step_name: str, h5_image_name: Optional[str] = None) -> dict:
        """开始执行处理步骤"""
        try:
            logger.info(f"开始执行步骤: {pipeline_id}/{step_name}")
            
            # 检查是否已有该步骤在运行
            job_key = f"{pipeline_id}_{step_name}"
            if job_key in self.running_jobs:
                logger.warning(f"步骤 {pipeline_id}/{step_name} 已在运行中")
                return {"status": "already_running", "job_id": self.running_jobs[job_key].job_id}
            
            # 提交 sbatch 作业
            job_id = await self._submit_sbatch_job(pipeline_id, step_name, h5_image_name)
            
            if job_id:
                # 记录作业信息
                job_info = JobInfo(
                    job_id=job_id,
                    pipeline_id=pipeline_id,
                    step_name=step_name,
                    submit_time=datetime.now(),
                    last_check_time=datetime.now(),
                    h5_image_name=h5_image_name
                )
                self.running_jobs[job_key] = job_info
                
                # 保存新任务到文件
                self._save_running_jobs()

                # 通知 core_server_api 步骤已开始
                await self._notify_step_started(pipeline_id, step_name, job_id)
                
                logger.info(f"作业提交成功: {job_id} for {pipeline_id}/{step_name}")
                return {"status": "submitted", "job_id": job_id}
            else:
                error_msg = f"作业提交失败: {pipeline_id}/{step_name}"
                logger.error(error_msg)
                # 通知步骤失败
                await self._notify_step_failed(pipeline_id, step_name, error_msg)
                return {"status": "failed", "error": "作业提交失败"}
                
        except Exception as e:
            logger.error(f"开始步骤时发生错误: {e}")
            await self._notify_step_failed(pipeline_id, step_name, str(e))
            return {"status": "error", "error": str(e)}
    
    async def _submit_sbatch_job(self, pipeline_id: str, step_name: str, h5_image_name: Optional[str]) -> Optional[str]:
        """提交 sbatch 作业"""
        try:
            # 根据步骤名称选择对应的处理脚本
            script_mapping = {
                "mip_generation": "pipeline_stage_h5_to_mip.py",
                "h5_to_v3draw": "pipeline_stage_h5_to_v3draw.py",
                "bit_conversion": "pipeline_stage_16bit_to_8bit.py",
                "downsample": "pipeline_stage_8bit_downsample.py",
                "cell_crop_generation": "pipeline_stage_cell_crop_generation.py",
            }
            
            script_file = script_mapping.get(step_name)
            if not script_file:
                raise ValueError(f"未知的步骤名称: {step_name}")
            
            # 检查脚本文件是否存在
            script_path = os.path.join(os.path.dirname(__file__), script_file)
            if not os.path.exists(script_path):
                raise FileNotFoundError(f"处理脚本不存在: {script_path}")
            
            image_name = h5_image_name if h5_image_name is not None else ""
            if step_name == "h5_to_v3draw" or step_name == "mip_generation":
                image_name = h5_image_name if h5_image_name is not None else ""
            elif step_name == "bit_conversion" and image_name:
                image_name = image_name.replace('.pyramid.h5', '.v3draw')
            elif step_name == "downsample" and image_name:
                image_name = image_name.replace('.pyramid.h5', '_8bit.v3draw')

            temp_script_root_path = cfg.SlurmScriptPath
            os.makedirs(temp_script_root_path, exist_ok=True)

            # 构建 sbatch 命令
            sbatch_script = self._generate_sbatch_script(
                pipeline_id, step_name, script_path, image_name, temp_script_root_path)

            # 写入临时脚本文件
            temp_script_path = f"{temp_script_root_path}/sbatch_{pipeline_id}_{step_name}_{int(time.time())}.sh"
            with open(temp_script_path, 'w') as f:
                f.write(sbatch_script)

            # chmod +x 使脚本可执行
            os.chmod(temp_script_path, 0o755)
            logger.info(f"提交 sbatch 作业: {temp_script_path}")

            # 提交作业
            cmd = ["sbatch", temp_script_path]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # 解析作业ID
            output = result.stdout.strip()
            if "Submitted batch job" in output:
                job_id = output.split()[-1]
                logger.info(f"Sbatch 作业提交成功: {job_id}")
                return job_id
            else:
                logger.error(f"无法解析作业ID: {output}")
                return None
                
        except subprocess.CalledProcessError as e:
            logger.error(f"Sbatch 命令执行失败: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"提交 sbatch 作业时发生错误: {e}")
            return None

    def _generate_sbatch_script(self, pipeline_id: str, step_name: str, script_path: str, image_name: str, temp_script_root_path: str) -> str:
        """生成 sbatch 脚本内容"""
        image_path = os.path.join(cfg.ImageTransferTemp, image_name if image_name else "") 

        return f"""#!/bin/bash
#SBATCH --job-name={step_name}_{pipeline_id}
#SBATCH --output={temp_script_root_path}/slurm_%j.out
#SBATCH --error={temp_script_root_path}/slurm_%j.err
#SBATCH --qos=pbstorageaccess
#SBATCH --partition=normal
#SBATCH --mem=250G
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --ntasks-per-node=1

# 设置环境变量
export PIPELINE_ID={pipeline_id}
export STEP_NAME={step_name}
export WORKER_ID={self.worker_id}
export CORE_SERVER_URL={self.core_server_url}

# 执行处理脚本
source $(conda info --base)/etc/profile.d/conda.sh
conda activate HumanDatabaseTools
python3 {script_path} --image-path "{image_path}"
"""
    
    async def _check_job_status(self, job_info: JobInfo) -> str:
        """检查作业状态"""
        try:
            cmd = ["squeue", "-j", job_info.job_id, "-h", "-o", "%T"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0 and result.stdout.strip():
                status = result.stdout.strip()
                return status
            else:
                # 作业不在队列中，检查是否已完成
                cmd = ["sacct", "-j", job_info.job_id, "-n", "-o", "State"]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
                
                if result.returncode == 0 and result.stdout.strip():
                    status = result.stdout.strip().split()[0]
                    return status
                else:
                    return "UNKNOWN"
                    
        except subprocess.TimeoutExpired:
            logger.warning(f"检查作业状态超时: {job_info.job_id}")
            return "TIMEOUT"
        except Exception as e:
            logger.error(f"检查作业状态时发生错误: {e}")
            return "ERROR"
    
    async def _monitor_jobs(self):
        """监控所有运行中的作业"""
        while self.is_running:
            try:
                # 检查 http_client 是否可用
                if self.http_client.is_closed:
                    logger.debug("HTTP 客户端已关闭，停止作业监控")
                    break
                    
                jobs_to_remove = []
                
                for job_key, job_info in self.running_jobs.items():
                    # 检查作业状态
                    current_status = await self._check_job_status(job_info)
                    
                    if current_status != job_info.status:
                        logger.info(f"作业状态变化: {job_info.job_id} {job_info.status} -> {current_status}")
                        job_info.status = current_status
                        
                        # 更新进度
                        await self._update_job_progress(job_info, current_status)

                        # 保存状态变化到文件
                        self._save_running_jobs()

                    # 处理已完成或失败的作业
                    final_states = ["COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY"]
                    if current_status in final_states:
                        await self._handle_job_completion(job_info, current_status)
                        jobs_to_remove.append(job_key)
                    
                    job_info.last_check_time = datetime.now()
                
                # 移除已完成的作业
                for job_key in jobs_to_remove:
                    del self.running_jobs[job_key]
                
                # 如果有任务被移除，更新保存的任务文件
                if jobs_to_remove:
                    self._save_running_jobs()

                await asyncio.sleep(self.job_check_interval)
                
            except Exception as e:
                logger.error(f"监控作业时发生错误: {e}")
                # 如果是因为事件循环关闭导致的错误，停止监控
                if "Event loop is closed" in str(e):
                    logger.debug("检测到事件循环已关闭，停止作业监控")
                    break
                await asyncio.sleep(self.job_check_interval)
    
    async def _update_job_progress(self, job_info: JobInfo, status: str):
        """更新作业进度"""
        try:
            # 检查 worker 是否仍在运行且 http_client 可用
            if not self.is_running or self.http_client.is_closed:
                logger.debug(f"跳过进度更新，worker 已关闭或 http_client 不可用: {job_info.job_id}")
                return
                
            # 映射 SLURM 状态到步骤状态
            step_status = map_slurm_status_to_step_status(status)
            
            # 根据状态设置进度
            progress = 0
            if step_status == StepStatusEnum.RUNNING.value:
                progress = 50  # 可以根据实际情况调整
            elif step_status == StepStatusEnum.COMPLETED.value:
                progress = 100
            elif step_status == StepStatusEnum.FAILED.value:
                # 如果之前在运行，保持进度；否则设为0
                progress = 50 if job_info.status == "RUNNING" else 0
            
            update_data = {
                "status": step_status,
                "progress": progress,
                "error_message": ""
            }
            
            url = f"{self.core_server_url}/api/pipeline/{job_info.pipeline_id}/step/{job_info.step_name}"
            response = await self.http_client.put(url, json=update_data)
            
            if response.status_code == 200:
                logger.debug(f"进度更新成功: {job_info.job_id}")
            else:
                logger.warning(f"进度更新失败: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"更新作业进度时发生错误: {e}")
            # 如果是因为事件循环关闭导致的错误，不再尝试重新连接
            if "Event loop is closed" in str(e):
                logger.debug("检测到事件循环已关闭，停止进度更新")
                return
    
    async def _handle_job_completion(self, job_info: JobInfo, final_status: str):
        """处理作业完成"""
        try:
            # 检查 worker 是否仍在运行且 http_client 可用
            if not self.is_running or self.http_client.is_closed:
                logger.debug(f"跳过作业完成处理，worker 已关闭或 http_client 不可用: {job_info.job_id}")
                return
                
            if final_status == "COMPLETED":
                # 根据步骤名称组成对应的文件名
                h5_image_name = job_info.h5_image_name if job_info.h5_image_name is not None else ""
                script_mapping = {
                    "mip_generation": f"{h5_image_name.replace('.pyramid.h5', '_MIP.tif')}",
                    "h5_to_v3draw": f"{h5_image_name.replace('.pyramid.h5', '.v3draw')}",
                    "bit_conversion": f"{h5_image_name.replace('.pyramid.h5', '_8bit.v3draw')}",
                    "downsample": f"{h5_image_name.replace('.pyramid.h5', '_8bit_downsampled.v3draw')}",
                }


                file_to_check = script_mapping.get(job_info.step_name, "")
                if file_to_check != "":
                    if not os.path.exists(os.path.join(cfg.ImageTransferTemp, file_to_check)):
                        # 构建详细的错误信息
                        error_message = f"SLURM作业失败 - 作业ID: {job_info.job_id}, 最终状态: {final_status}, 任务结束但未找到处理后的结果文件！"
                        
                        # 尝试获取更详细的错误信息
                        try:
                            error_details = await self._get_job_error_details(job_info.job_id)
                            if error_details:
                                error_message += f", 错误详情: {error_details}"
                        except Exception as e:
                            logger.debug(f"获取作业错误详情失败: {e}")
                        
                        # 调用失败API
                        await self._notify_step_failed(
                            job_info.pipeline_id, 
                            job_info.step_name,
                            error_message
                        )
                        return
                    elif job_info.step_name == "mip_generation":
                        # Archive files sequentially - ensure organize_image_files completes before ArchiveCommand
                        # Using await asyncio.to_thread to run blocking functions without blocking the event loop
                        # 提取图像ID用于单文件处理
                        h5_image_name = job_info.h5_image_name if job_info.h5_image_name else ""

                        # 从h5文件名中提取图像ID
                        import re
                        pattern = r'(P\d+-T\d+-R\d+-S\d+(?:-B\d+)?(?:-\d+)?)'
                        match = re.search(pattern, h5_image_name)
                        image_id = match.group(1) if match else None

                        # 只处理当前文件相关的文件
                        await asyncio.to_thread(archive.organize_image_files, h5_image_name)
                        # Only execute ArchiveCommand after organize_image_files is complete
                        await asyncio.to_thread(archive.ArchiveCommand, image_id)

                # 调用完成API
                url = f"{self.core_server_url}/api/pipeline/{job_info.pipeline_id}/step/{job_info.step_name}/complete"
                complete_data = {
                    "job_id": job_info.job_id,
                    "completion_time": datetime.now().isoformat()
                }
                response = await self.http_client.post(url, json=complete_data)
                
                if response.status_code == 200:
                    logger.info(f"步骤完成通知发送成功: {job_info.pipeline_id}/{job_info.step_name}")
                else:
                    logger.warning(f"步骤完成通知发送失败: {response.status_code}")
            else:
                # 构建详细的错误信息
                error_message = f"SLURM作业失败 - 作业ID: {job_info.job_id}, 最终状态: {final_status}"
                
                # 尝试获取更详细的错误信息
                try:
                    error_details = await self._get_job_error_details(job_info.job_id)
                    if error_details:
                        error_message += f", 错误详情: {error_details}"
                except Exception as e:
                    logger.debug(f"获取作业错误详情失败: {e}")
                
                # 调用失败API
                await self._notify_step_failed(
                    job_info.pipeline_id, 
                    job_info.step_name, 
                    error_message
                )
                
        except Exception as e:
            logger.error(f"处理作业完成时发生错误: {e}")
    
    async def _notify_step_started(self, pipeline_id: str, step_name: str, job_id: str):
        """通知步骤已开始"""
        try:
            # 检查 worker 是否仍在运行且 http_client 可用
            if not self.is_running or self.http_client.is_closed:
                logger.debug(f"跳过步骤开始通知，worker 已关闭或 http_client 不可用: {pipeline_id}/{step_name}")
                return
                
            url = f"{self.core_server_url}/api/pipeline/{pipeline_id}/step/{step_name}"
            update_data = {
                "status": StepStatusEnum.RUNNING.value,
                "start_time": datetime.now().isoformat(),
                "progress": 0,
                "job_id": job_id
            }
            
            response = await self.http_client.put(url, json=update_data)
            
            if response.status_code == 200:
                logger.info(f"步骤开始通知发送成功: {pipeline_id}/{step_name}")
            else:
                logger.warning(f"步骤开始通知发送失败: {response.status_code}")
                
        except Exception as e:
            logger.error(f"通知步骤开始时发生错误: {e}")
            # 如果是因为事件循环关闭导致的错误，不再尝试重新连接
            if "Event loop is closed" in str(e):
                logger.debug("检测到事件循环已关闭，停止步骤开始通知")
                return
    
    async def _notify_step_failed(self, pipeline_id: str, step_name: str, error_message: str):
        """通知步骤失败"""
        try:
            # 检查 worker 是否仍在运行且 http_client 可用
            if not self.is_running or self.http_client.is_closed:
                logger.debug(f"跳过步骤失败通知，worker 已关闭或 http_client 不可用: {pipeline_id}/{step_name}")
                logger.error(f"原始失败信息: {error_message}")
                return
                
            url = f"{self.core_server_url}/api/pipeline/{pipeline_id}/step/{step_name}/fail"
            error_data = {
                "error_message": error_message,
                "failure_time": datetime.now().isoformat()
            }
            
            # 根据 core_server_api 的要求，将 error_data 作为请求体发送
            response = await self.http_client.post(url, json=error_data)
            
            if response.status_code == 200:
                logger.info(f"步骤失败通知发送成功: {pipeline_id}/{step_name}")
                logger.info(f"失败信息: {error_message}")
            else:
                logger.warning(f"步骤失败通知发送失败: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"通知步骤失败时发生错误: {e}")
            # 确保即使通知失败，也要记录原始错误信息
            logger.error(f"原始失败信息: {error_message}")
            # 如果是因为事件循环关闭导致的错误，不再尝试重新连接
            if "Event loop is closed" in str(e):
                logger.debug("检测到事件循环已关闭，停止步骤失败通知")
                return
    
    async def _get_job_error_details(self, job_id: str) -> Optional[str]:
        """获取作业的详细错误信息"""
        try:
            # 尝试从 SLURM 获取作业的详细信息
            cmd = ["sacct", "-j", job_id, "-l", "-n"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0 and result.stdout.strip():
                # 解析输出中的错误信息
                lines = result.stdout.strip().split('\n')
                if lines:
                    # 通常第一行包含主要的作业信息
                    job_info_line = lines[0]
                    # 这里可以根据需要解析更多信息
                    return f"SLURM作业详情: {job_info_line[:200]}..."  # 限制长度
            
            # 如果 sacct 没有返回有用信息，尝试查看错误日志
            error_log_path = f"/tmp/slurm_{job_id}.err"
            if os.path.exists(error_log_path):
                with open(error_log_path, 'r') as f:
                    error_content = f.read()
                    if error_content.strip():
                        # 返回错误日志的前几行
                        error_lines = error_content.strip().split('\n')
                        return ' '.join(error_lines[:3])  # 取前3行
            
            return None
            
        except subprocess.TimeoutExpired:
            logger.debug(f"获取作业 {job_id} 错误详情超时")
            return None
        except Exception as e:
            logger.debug(f"获取作业 {job_id} 错误详情时发生异常: {e}")
            return None
    
    async def _send_heartbeat(self):
        """发送心跳"""
        while self.is_running:
            try:
                # 检查 worker 是否仍在运行且 http_client 可用
                if not self.is_running or self.http_client.is_closed:
                    logger.debug("跳过心跳发送，worker 已关闭或 http_client 不可用")
                    break
                    
                heartbeat_data = {
                    "worker_id": self.worker_id,
                    "timestamp": datetime.now().isoformat(),
                    "running_jobs": len(self.running_jobs),
                    "status": "alive"
                }
                
                # 这里需要根据实际的心跳API端点调整
                url = f"{self.core_server_url}/api/worker/heartbeat"
                response = await self.http_client.post(url, json=heartbeat_data)
                
                if response.status_code == 200:
                    logger.debug(f"心跳发送成功")
                else:
                    logger.warning(f"心跳发送失败: {response.status_code}")
                    
            except Exception as e:
                logger.error(f"发送心跳时发生错误: {e}")
                # 如果是因为事件循环关闭导致的错误，停止心跳
                if "Event loop is closed" in str(e):
                    logger.debug("检测到事件循环已关闭，停止心跳发送")
                    break
            
            await asyncio.sleep(self.heartbeat_interval)
    
    async def get_worker_status(self) -> dict:
        """获取 worker 状态"""
        return {
            "worker_id": self.worker_id,
            "is_running": self.is_running,
            "running_jobs_count": len(self.running_jobs),
            "running_jobs": [
                {
                    "job_id": job.job_id,
                    "pipeline_id": job.pipeline_id,
                    "step_name": job.step_name,
                    "status": job.status,
                    "submit_time": job.submit_time.isoformat(),
                    "last_check_time": job.last_check_time.isoformat(),
                    "h5_image_name": job.h5_image_name
                }
                for job in self.running_jobs.values()
            ],
            "timestamp": datetime.now().isoformat()
        }
    
    async def shutdown(self):
        """关闭 worker"""
        logger.info("开始关闭 Pipeline Worker...")
        
        # 停止后台任务
        self.is_running = False
        
        # 等待后台任务完成
        if self.background_tasks:
            logger.info("等待后台任务完成...")
            # 给后台任务一些时间来检查 is_running 状态
            await asyncio.sleep(1)
            
            # 取消所有后台任务
            for task in self.background_tasks:
                if not task.done():
                    task.cancel()
            
            # 等待所有任务完成或被取消
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
            logger.info("后台任务已停止")
        
        # 关闭 HTTP 客户端
        if not self.http_client.is_closed:
            await self.http_client.aclose()
            logger.info("HTTP 客户端已关闭")
        
        # 清理运行任务文件（如果没有运行中的任务）
        if not self.running_jobs:
            self._cleanup_running_jobs_file()

        logger.info("Pipeline Worker 已关闭")

# FastAPI 应用
app = FastAPI(title="Pipeline Worker", version="1.0.0")

# 全局 worker 实例
worker = None
# 全局文件监控器实例
file_observer = None

@app.on_event("startup")
async def startup_event():
    """启动事件"""
    global worker, file_observer
    
    # 从环境变量或配置读取参数
    core_server_url = cfg.CoreServerURL
    worker_id = f"worker_{os.getpid()}"
    
    worker = PipelineWorker(
        core_server_url=core_server_url,
        worker_id=worker_id
    )
    
    # 启动后台任务并保存任务引用
    monitor_task = asyncio.create_task(worker._monitor_jobs())
    heartbeat_task = asyncio.create_task(worker._send_heartbeat())
    
    # 验证恢复的任务
    if worker.running_jobs:
        verify_task = asyncio.create_task(worker._verify_recovered_jobs())
        worker.background_tasks = [monitor_task, heartbeat_task, verify_task]
    else:
        worker.background_tasks = [monitor_task, heartbeat_task]
    
    # 启动文件监控器
    file_observer = UploadFileWatcher()
    
    logger.info("Pipeline Worker 启动完成")


def UploadFileWatcher():
    """
    监听cfg.ImageTransferTemp路径下的.h5文件上传
    使用轮询方式检测新的.h5文件，支持NFS挂载的文件系统
    """
    
    class H5FilePollingWatcher:
        """H5文件轮询监控器"""
        
        def __init__(self, watch_path: str, poll_interval: int = 5):
            self.watch_path = watch_path
            self.poll_interval = poll_interval
            self.logger = logging.getLogger(f"{__name__}.H5FilePollingWatcher")
            self.processed_files_json = "processed_h5_files.json"
            self.is_running = True

            # 确保监控目录存在
            if not os.path.exists(self.watch_path):
                self.logger.warning(f"监控路径不存在: {self.watch_path}")
                return

            self.logger.info(f"初始化H5文件轮询监控器，监控路径: {self.watch_path}")
        
        def _load_processed_files(self) -> Dict[str, str]:
            """加载已处理文件记录"""
            try:
                if os.path.exists(self.processed_files_json):
                    with open(self.processed_files_json, 'r', encoding='utf-8') as f:
                        return json.load(f)
                return {}
            except Exception as e:
                self.logger.error(f"加载已处理文件记录失败: {e}")
                return {}

        def _save_processed_files(self, processed_files: Dict[str, str]):
            """保存已处理文件记录，同时清理30天前的记录"""
            try:
                # 清理30天前的记录
                cutoff_date = (datetime.now() - timedelta(days=30)).isoformat()
                cleaned_files = {
                    filename: timestamp
                    for filename, timestamp in processed_files.items()
                    if timestamp > cutoff_date
                }

                # 保存到文件
                with open(self.processed_files_json, 'w', encoding='utf-8') as f:
                    json.dump(cleaned_files, f, ensure_ascii=False, indent=2)

                cleaned_count = len(processed_files) - len(cleaned_files)
                if cleaned_count > 0:
                    self.logger.info(f"清理了 {cleaned_count} 个30天前的处理记录")

            except Exception as e:
                self.logger.error(f"保存已处理文件记录失败: {e}")

        def _get_h5_files(self) -> List[str]:
            """获取目录下所有.h5文件"""
            try:
                h5_files = []
                for root, dirs, files in os.walk(self.watch_path):
                    for file in files:
                        if file.endswith('.h5'):
                            file_path = os.path.join(root, file)
                            h5_files.append(file_path)
                return h5_files
            except Exception as e:
                self.logger.error(f"遍历H5文件时发生错误: {e}")
                return []

        def _is_file_stable(self, file_path: str, stability_checks: int = 3) -> bool:
            """检查文件是否稳定（上传完成）"""
            try:
                if not os.path.exists(file_path):
                    return False

                # 获取初始文件大小
                initial_size = os.path.getsize(file_path)
                if initial_size == 0:
                    return False

                # 多次检查文件大小是否稳定
                for _ in range(stability_checks):
                    time.sleep(1)
                    if not os.path.exists(file_path):
                        return False

                    current_size = os.path.getsize(file_path)
                    if current_size != initial_size:
                        return False

                return True

            except Exception as e:
                self.logger.error(f"检查文件稳定性时发生错误: {e}")
                return False
        
        def _handle_h5_file(self, file_path: str):
            """处理检测到的H5文件"""
            try:
                global worker

                # 获取文件名（不包含路径）
                file_name = os.path.basename(file_path)
                self.logger.info(f"处理H5文件: {file_name}")
                
                # 检查文件是否稳定（上传完成）
                if not self._is_file_stable(file_path):
                    self.logger.debug(f"文件还在上传中，跳过: {file_name}")
                    return

                self.logger.info(f"H5文件上传完成: {file_name}")
                
                # 检查 worker 是否可用
                if worker is None:
                    self.logger.error("Worker 实例不可用，无法创建流程")
                    return
                
                if worker.http_client.is_closed or not worker.is_running:
                    self.logger.error("Worker 已关闭或HTTP 客户端不可用，无法创建流程")
                    return
                
                # 保存core_server_url以避免在线程中访问worker实例
                core_server_url = worker.core_server_url
                
                # 使用线程池来处理异步任务，避免事件循环冲突
                import threading
                
                def create_pipeline_async():
                    """在新线程中创建流程"""
                    loop = None
                    try:
                        # 创建新的事件循环
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        
                        create_data = {
                            "h5_img_name": f"{file_name}",
                            "uploader": "示例用户",
                            "notification_emails": [],  # 请替换为您的邮箱
                            "wait_for_image_upload": False
                        }
                        
                        url = f"{core_server_url}/api/pipeline/create"
                        
                        # 创建新的HTTP客户端用于此请求
                        async def make_request():
                            async with httpx.AsyncClient(timeout=30.0) as client:
                                response = await client.post(url, json=create_data)
                                if response.status_code == 200:
                                    pipeline_data = response.json()
                                    pipeline_id = pipeline_data["pipeline_id"]
                                    self.logger.info(f"✅ 流程创建成功！流程ID: {pipeline_id}")

                                    # 记录已处理的文件
                                    processed_files = self._load_processed_files()
                                    processed_files[file_name] = datetime.now(
                                    ).isoformat()
                                    self._save_processed_files(processed_files)

                                    return True
                                else:
                                    self.logger.error(f"❌ 流程创建失败: {response.text}")
                                    return False
                        
                        # 运行异步请求
                        success = loop.run_until_complete(make_request())
                        return success
                        
                    except Exception as e:
                        self.logger.error(f"创建流程时发生错误: {e}")
                        return False
                    finally:
                        if loop is not None:
                            try:
                                loop.close()
                            except Exception:
                                pass
                
                # 在新线程中运行异步操作
                thread = threading.Thread(target=create_pipeline_async)
                thread.daemon = True
                thread.start()
                thread.join(timeout=60)  # 等待最多60秒
                            
            except Exception as e:
                self.logger.error(f"处理H5文件时发生错误: {e}")
        
        def start_polling(self):
            """开始轮询监控"""
            self.logger.info(f"开始轮询监控H5文件，间隔: {self.poll_interval}秒")

            def polling_loop():
                """轮询循环"""
                while self.is_running:
                    try:
                        # 加载已处理文件记录
                        processed_files = self._load_processed_files()
                        
                        # 获取当前目录下的所有H5文件
                        current_files = self._get_h5_files()
                        
                        # 检查新文件
                        for file_path in current_files:
                            file_name = os.path.basename(file_path)

                            # 检查是否已经处理过
                            if file_name not in processed_files:
                                self.logger.info(f"发现新的H5文件: {file_name}")
                                self._handle_h5_file(file_path)

                        # 等待下次轮询
                        time.sleep(self.poll_interval)

                    except Exception as e:
                        self.logger.error(f"轮询过程中发生错误: {e}")
                        time.sleep(self.poll_interval)

            # 在后台线程中运行轮询
            import threading
            self.polling_thread = threading.Thread(target=polling_loop)
            self.polling_thread.daemon = True
            self.polling_thread.start()

        def stop(self):
            """停止轮询监控"""
            self.is_running = False
            self.logger.info("H5文件轮询监控已停止")
    
    # 创建并启动轮询监控器
    watch_path = cfg.ImageTransferTemp
    
    if not os.path.exists(watch_path):
        logger.warning(f"监控路径不存在: {watch_path}")
        return None
    
    watcher = H5FilePollingWatcher(watch_path)
    watcher.start_polling()
    
    return watcher


@app.on_event("shutdown")
async def shutdown_event():
    """关闭事件"""
    global worker, file_observer
    
    # 停止文件监控器
    if file_observer:
        file_observer.stop()
        logger.info("文件监控器已停止")
    
    # 关闭worker
    if worker:
        await worker.shutdown()

@app.post("/api/worker/start")
async def start_step_endpoint(
    request: StartStepRequest
):
    print(f"接收到开始步骤请求: {request.pipeline_id}/{request.step_name} - {request.h5_image_name}")
    """接收步骤开始请求"""
    if not worker:
        raise HTTPException(status_code=500, detail="Worker 未初始化")
    
    # 同步处理步骤开始，以便返回准确的状态
    result = await worker.start_step(request.pipeline_id, request.step_name, request.h5_image_name)
    
    # 根据返回结果构造响应
    if result["status"] == "already_running":
        return {
            "message": f"步骤 {request.step_name} 已在运行中",
            "pipeline_id": request.pipeline_id,
            "status": "already_running",
            "job_id": result.get("job_id")
        }
    elif result["status"] == "submitted":
        return {
            "message": f"步骤 {request.step_name} 开始请求已提交",
            "pipeline_id": request.pipeline_id,
            "status": "submitted",
            "job_id": result.get("job_id")
        }
    elif result["status"] == "failed":
        raise HTTPException(
            status_code=500, 
            detail=f"步骤提交失败: {result.get('error', '未知错误')}"
        )
    else:
        raise HTTPException(
            status_code=500, 
            detail=f"步骤处理出错: {result.get('error', '未知错误')}"
        )

@app.get("/api/worker/status")
async def get_worker_status():
    """获取 worker 状态"""
    if not worker:
        raise HTTPException(status_code=500, detail="Worker 未初始化")
    
    return await worker.get_worker_status()

@app.get("/api/worker/file-watcher/status")
async def get_file_watcher_status():
    """获取文件监控器状态"""
    global file_observer
    
    if not file_observer:
        return {
            "status": "not_running",
            "watch_path": cfg.ImageTransferTemp,
            "message": "文件监控器未启动"
        }
    
    return {
        "status": "running" if file_observer.is_running else "stopped",
        "watch_path": cfg.ImageTransferTemp,
        "is_running": file_observer.is_running,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/worker/health")
async def health_check():
    """健康检查"""
    return {
        "status": "healthy",
        "worker_id": worker.worker_id if worker else "unknown",
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Pipeline Worker")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=7000, help="Port to bind to")
    
    args = parser.parse_args()
    
    uvicorn.run(app, host=args.host, port=args.port)
