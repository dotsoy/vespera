#!/usr/bin/env python3
"""
Marimo笔记本启动器
用于从Streamlit界面启动Marimo研究笔记本
"""
import subprocess
import sys
import os
import time
import socket
from pathlib import Path
from typing import Optional
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

logger = get_logger("marimo_launcher")


class MarimoLauncher:
    """Marimo启动器"""
    
    def __init__(self):
        self.base_port = 8081
        self.max_port = 8090
        self.notebooks_dir = project_root / "notebooks"
        self.running_processes = {}
        
    def find_available_port(self, start_port: int = None) -> int:
        """查找可用端口"""
        start_port = start_port or self.base_port
        
        for port in range(start_port, self.max_port + 1):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(('localhost', port))
                    return port
            except OSError:
                continue
        
        raise RuntimeError(f"无法找到可用端口 ({start_port}-{self.max_port})")
    
    def check_marimo_installed(self) -> bool:
        """检查Marimo是否已安装"""
        try:
            result = subprocess.run(
                ["marimo", "--version"], 
                capture_output=True, 
                text=True, 
                timeout=5
            )
            return result.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False
    
    def create_notebooks_directory(self):
        """创建notebooks目录（如果不存在）"""
        if not self.notebooks_dir.exists():
            self.notebooks_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"创建notebooks目录: {self.notebooks_dir}")
            
            # 创建示例笔记本
            self.create_sample_notebooks()
    
    def create_sample_notebooks(self):
        """创建示例研究笔记本"""
        sample_notebooks = {
            "strategy_backtest_template.py": '''"""
策略回测模板
用于测试和验证交易策略
"""
import marimo as mo
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta

def __():
    mo.md("""
    # 📈 策略回测模板
    
    这是一个用于策略回测的Marimo笔记本模板。
    您可以在这里测试和验证各种交易策略。
    """)

def __():
    # 策略参数设置
    mo.md("## ⚙️ 策略参数")
    
    # 创建参数输入控件
    lookback_period = mo.ui.slider(5, 60, value=20, label="回看周期")
    threshold = mo.ui.slider(0.01, 0.1, value=0.05, step=0.01, label="信号阈值")
    
    return lookback_period, threshold

def __(lookback_period, threshold):
    mo.md(f"""
    当前参数设置：
    - 回看周期: {lookback_period.value} 天
    - 信号阈值: {threshold.value:.2%}
    """)

def __():
    mo.md("""
    ## 📊 数据加载
    
    在这里加载股票数据进行回测分析。
    """)
    
    # 示例数据生成
    dates = pd.date_range(start='2024-01-01', end='2024-06-01', freq='D')
    prices = 100 + np.cumsum(np.random.randn(len(dates)) * 0.02)
    
    data = pd.DataFrame({
        'date': dates,
        'price': prices,
        'returns': np.concatenate([[0], np.diff(prices) / prices[:-1]])
    })
    
    return data

def __(data):
    # 创建价格图表
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=data['date'],
        y=data['price'],
        mode='lines',
        name='价格',
        line=dict(color='blue')
    ))
    
    fig.update_layout(
        title="股票价格走势",
        xaxis_title="日期",
        yaxis_title="价格",
        template="plotly_white"
    )
    
    mo.ui.plotly(fig)

def __():
    mo.md("""
    ## 🎯 策略信号
    
    在这里实现您的交易策略逻辑。
    """)

def __():
    mo.md("""
    ## 📈 回测结果
    
    显示策略的回测结果和性能指标。
    """)

if __name__ == "__main__":
    mo.run()
''',
            
            "specific_stock_deep_dive.py": '''"""
个股深度分析
专门用于单只股票的深入研究
"""
import marimo as mo
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def __():
    mo.md("""
    # 🔍 个股深度分析
    
    这个笔记本专门用于对单只股票进行深入的技术和基本面分析。
    """)

def __():
    mo.md("## 📝 股票选择")
    
    # 股票代码输入
    stock_code = mo.ui.text(
        value="000001.SZ",
        label="股票代码",
        placeholder="请输入股票代码，如：000001.SZ"
    )
    
    return stock_code

def __(stock_code):
    mo.md(f"当前分析股票: **{stock_code.value}**")

def __():
    mo.md("""
    ## 📊 技术分析
    
    包含K线图、技术指标、成交量分析等。
    """)

def __():
    mo.md("""
    ## 💰 基本面分析
    
    财务指标、估值分析、行业对比等。
    """)

def __():
    mo.md("""
    ## 🎯 投资建议
    
    基于技术面和基本面的综合投资建议。
    """)

if __name__ == "__main__":
    mo.run()
''',
            
            "capital_flow_validation.py": '''"""
资金流向验证
验证和分析市场资金流向
"""
import marimo as mo
import pandas as pd
import numpy as np
import plotly.express as px

def __():
    mo.md("""
    # 💰 资金流向验证分析
    
    这个笔记本用于验证和分析市场的资金流向情况。
    """)

def __():
    mo.md("## 📈 市场资金流向")

def __():
    mo.md("## 🏭 板块资金流向")

def __():
    mo.md("## 🔍 个股资金流向")

def __():
    mo.md("## 📊 资金流向指标验证")

if __name__ == "__main__":
    mo.run()
''',
            
            "market_sentiment_analysis.py": '''"""
市场情绪分析
分析市场整体情绪和投资者行为
"""
import marimo as mo
import pandas as pd
import numpy as np

def __():
    mo.md("""
    # 😊 市场情绪分析
    
    分析市场整体情绪、投资者行为和市场心理。
    """)

def __():
    mo.md("## 📊 情绪指标")

def __():
    mo.md("## 📈 恐慌贪婪指数")

def __():
    mo.md("## 🔄 市场轮动分析")

if __name__ == "__main__":
    mo.run()
'''
        }
        
        for filename, content in sample_notebooks.items():
            notebook_path = self.notebooks_dir / filename
            if not notebook_path.exists():
                with open(notebook_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                logger.info(f"创建示例笔记本: {filename}")
    
    def launch_notebook(self, notebook_name: str) -> dict:
        """启动Marimo笔记本"""
        try:
            # 检查Marimo是否安装
            if not self.check_marimo_installed():
                return {
                    'success': False,
                    'message': 'Marimo未安装。请运行: pip install marimo',
                    'port': None,
                    'url': None
                }
            
            # 确保notebooks目录存在
            self.create_notebooks_directory()
            
            # 构建笔记本文件路径
            notebook_path = self.notebooks_dir / notebook_name
            
            if not notebook_path.exists():
                return {
                    'success': False,
                    'message': f'笔记本文件不存在: {notebook_name}',
                    'port': None,
                    'url': None
                }
            
            # 查找可用端口
            try:
                port = self.find_available_port()
            except RuntimeError as e:
                return {
                    'success': False,
                    'message': str(e),
                    'port': None,
                    'url': None
                }
            
            # 构建Marimo命令
            command = [
                "marimo", "edit", str(notebook_path),
                "--port", str(port),
                "--host", "localhost"
                # 移除 --headless，让Marimo正常运行
            ]
            
            logger.info(f"启动Marimo笔记本: {notebook_name} 在端口 {port}")
            
            # 启动Marimo进程
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # 等待一下确保进程启动
            time.sleep(2)
            
            # 检查进程是否成功启动
            if process.poll() is not None:
                # 进程已经结束，可能启动失败
                stdout, stderr = process.communicate()
                return {
                    'success': False,
                    'message': f'启动失败: {stderr or stdout}',
                    'port': None,
                    'url': None
                }
            
            # 记录运行中的进程
            self.running_processes[notebook_name] = {
                'process': process,
                'port': port,
                'start_time': datetime.now()
            }
            
            url = f"http://localhost:{port}"
            
            return {
                'success': True,
                'message': f'Marimo笔记本已启动',
                'port': port,
                'url': url,
                'notebook': notebook_name
            }
            
        except Exception as e:
            logger.error(f"启动Marimo笔记本失败: {e}")
            return {
                'success': False,
                'message': f'启动失败: {str(e)}',
                'port': None,
                'url': None
            }
    
    def stop_notebook(self, notebook_name: str) -> dict:
        """停止Marimo笔记本"""
        if notebook_name not in self.running_processes:
            return {
                'success': False,
                'message': f'笔记本 {notebook_name} 未在运行'
            }
        
        try:
            process_info = self.running_processes[notebook_name]
            process = process_info['process']
            
            # 终止进程
            process.terminate()
            
            # 等待进程结束
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # 强制杀死进程
                process.kill()
                process.wait()
            
            # 从运行列表中移除
            del self.running_processes[notebook_name]
            
            logger.info(f"已停止Marimo笔记本: {notebook_name}")
            
            return {
                'success': True,
                'message': f'笔记本 {notebook_name} 已停止'
            }
            
        except Exception as e:
            logger.error(f"停止Marimo笔记本失败: {e}")
            return {
                'success': False,
                'message': f'停止失败: {str(e)}'
            }
    
    def list_running_notebooks(self) -> list:
        """列出正在运行的笔记本"""
        running = []
        
        # 清理已结束的进程
        to_remove = []
        for notebook_name, process_info in self.running_processes.items():
            if process_info['process'].poll() is not None:
                to_remove.append(notebook_name)
            else:
                running.append({
                    'notebook': notebook_name,
                    'port': process_info['port'],
                    'url': f"http://localhost:{process_info['port']}",
                    'start_time': process_info['start_time']
                })
        
        # 移除已结束的进程
        for notebook_name in to_remove:
            del self.running_processes[notebook_name]
        
        return running
    
    def get_available_notebooks(self) -> list:
        """获取可用的笔记本列表"""
        self.create_notebooks_directory()
        
        notebooks = []
        for file_path in self.notebooks_dir.glob("*.py"):
            notebooks.append({
                'name': file_path.name,
                'path': str(file_path),
                'size': file_path.stat().st_size,
                'modified': datetime.fromtimestamp(file_path.stat().st_mtime)
            })
        
        return sorted(notebooks, key=lambda x: x['modified'], reverse=True)


def main():
    """命令行入口"""
    if len(sys.argv) < 2:
        print("用法: python launch_marimo.py <command> [notebook_name]")
        print("命令:")
        print("  launch <notebook_name>  - 启动笔记本")
        print("  stop <notebook_name>    - 停止笔记本")
        print("  list                    - 列出可用笔记本")
        print("  running                 - 列出运行中的笔记本")
        return
    
    launcher = MarimoLauncher()
    command = sys.argv[1]
    
    if command == "launch" and len(sys.argv) > 2:
        notebook_name = sys.argv[2]
        result = launcher.launch_notebook(notebook_name)
        
        if result['success']:
            print(f"✅ {result['message']}")
            print(f"📍 访问地址: {result['url']}")
        else:
            print(f"❌ {result['message']}")
    
    elif command == "stop" and len(sys.argv) > 2:
        notebook_name = sys.argv[2]
        result = launcher.stop_notebook(notebook_name)
        print(f"{'✅' if result['success'] else '❌'} {result['message']}")
    
    elif command == "list":
        notebooks = launcher.get_available_notebooks()
        print("📚 可用的Marimo笔记本:")
        for nb in notebooks:
            print(f"  • {nb['name']} (修改时间: {nb['modified'].strftime('%Y-%m-%d %H:%M')})")
    
    elif command == "running":
        running = launcher.list_running_notebooks()
        if running:
            print("🏃 运行中的笔记本:")
            for nb in running:
                print(f"  • {nb['notebook']} - {nb['url']} (启动时间: {nb['start_time'].strftime('%H:%M:%S')})")
        else:
            print("📭 没有运行中的笔记本")
    
    else:
        print("❌ 无效的命令或缺少参数")


if __name__ == "__main__":
    main()
