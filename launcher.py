"""5021_FundingRate-Predictor launcher — 啟動後端 + 前端（獨立子程序）"""
import subprocess
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
FLAGS = 0x08000000  # CREATE_NO_WINDOW

# 清除可能干擾子 Flask/FastAPI 的環境變數
env = os.environ.copy()
env.pop("WERKZEUG_SERVER_FD", None)
env.pop("WERKZEUG_RUN_MAIN", None)

# Backend (FastAPI on port 3021)
subprocess.Popen(
    [sys.executable, "main.py"],
    cwd=os.path.join(HERE, "backend"),
    creationflags=FLAGS,
    env=env,
    stdin=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)

# Frontend (Vite on port 5021)
subprocess.Popen(
    "npm run dev",
    cwd=os.path.join(HERE, "frontend"),
    shell=True,
    creationflags=FLAGS,
    env=env,
    stdin=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
