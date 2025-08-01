# 部署问题描述与解决方案
由于人脑数据库后端所在服务器与slurm提交任务的服务器不在同一台服务器上，并且两台服务器没有通过同一个物理链路进行连接，所以无法直接访问，尝试在allencenter配置iptables进行多个链路间进行转发失败，由于人脑数据库后端所在服务器已经通过ssh Tunnel将服务转发到跳板机（allencenter server），而slurm任务提交的服务器（slurm master node）与跳板机是通的，所以采用再一次通过ssh Tunnel（ssh -L 8000:192.168.4.47:8000 root@172.16.1.123）后直接在slurm master node使用localhost:8000即可正常连接后端服务

# Step 1: Properly Copy Your SSH Key to Server

## Create a fresh key if needed

ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""
 
## Make sure the .ssh directory exists on the server

ssh root@172.16.1.123 "mkdir -p ~/.ssh && chmod 700 ~/.ssh"
 
## Properly copy your public key to the server

cat ~/.ssh/id_rsa.pub | ssh root@172.16.1.123 "cat >> ~/.ssh/authorized_keys"
 
## Set correct permissions on the server

ssh root@172.16.1.123 "chmod 600 ~/.ssh/authorized_keys"

# Step 2: Verify Key Authentication Works

ssh -o PasswordAuthentication=no root@172.16.1.123 "echo SSH key authentication successful"

# Step 3: Update the SSH Tunnel Service
If the previous steps were successful, update the service:

sudo bash -c 'cat > /etc/systemd/system/ssh-tunnel.service << EOF
[Unit]
Description=SSH tunnel for service access
After=network.target
 
[Service]
Type=simple
ExecStart=/usr/bin/ssh -N -o ServerAliveInterval=60 -o ExitOnForwardFailure=yes -o GatewayPorts=yes -L 0.0.0.0:8000:192.168.4.47:8000 root@172.16.1.123
Restart=always
RestartSec=10
 
[Install]
WantedBy=multi-user.target
EOF'

sudo systemctl daemon-reload
sudo systemctl restart ssh-tunnel.service