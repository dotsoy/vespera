"""
Marimo研究室组件
在Streamlit中集成Marimo笔记本启动器
"""
import streamlit as st
import subprocess
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
import sys
sys.path.insert(0, str(project_root))

from scripts.launch_marimo import MarimoLauncher
from src.utils.logger import get_logger

logger = get_logger("marimo_lab_component")


class MarimoLabComponent:
    """Marimo研究室Streamlit组件"""
    
    def __init__(self):
        self.launcher = MarimoLauncher()
        
        # 初始化session state
        if 'marimo_running_notebooks' not in st.session_state:
            st.session_state.marimo_running_notebooks = {}
        
        if 'marimo_last_refresh' not in st.session_state:
            st.session_state.marimo_last_refresh = datetime.now()
    
    def render_sidebar(self):
        """在侧边栏渲染Marimo研究室"""
        with st.sidebar:
            st.markdown("---")
            st.header("🔬 Marimo 研究室")
            st.markdown("*交互式数据科学笔记本*")
            
            # 刷新按钮
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔄 刷新", key="marimo_refresh"):
                    self._refresh_notebook_status()
            
            with col2:
                if st.button("📚 管理", key="marimo_manage"):
                    st.session_state.show_marimo_manager = True
            
            # 显示可用笔记本
            self._render_available_notebooks()
            
            # 显示运行中的笔记本
            self._render_running_notebooks()
            
            # 快速创建笔记本
            self._render_quick_create()
    
    def render_main_panel(self):
        """在主面板渲染Marimo管理界面"""
        if st.session_state.get('show_marimo_manager', False):
            self._render_manager_panel()
    
    def _render_available_notebooks(self):
        """渲染可用笔记本列表"""
        st.subheader("📖 可用笔记本")
        
        try:
            notebooks = self.launcher.get_available_notebooks()
            
            if not notebooks:
                st.info("暂无笔记本文件")
                st.markdown("💡 点击下方'创建新笔记本'开始")
                return
            
            for notebook in notebooks:
                with st.container():
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        # 笔记本信息
                        st.write(f"**{notebook['name']}**")
                        st.caption(f"修改: {notebook['modified'].strftime('%m-%d %H:%M')}")
                    
                    with col2:
                        # 启动按钮
                        if st.button(
                            "🚀", 
                            key=f"launch_{notebook['name']}",
                            help=f"启动 {notebook['name']}",
                            use_container_width=True
                        ):
                            self._launch_notebook(notebook['name'])
                    
                    st.markdown("---")
        
        except Exception as e:
            st.error(f"加载笔记本列表失败: {e}")
            logger.error(f"加载笔记本列表失败: {e}")
    
    def _render_running_notebooks(self):
        """渲染运行中的笔记本"""
        running_notebooks = self.launcher.list_running_notebooks()
        
        if running_notebooks:
            st.subheader("🏃 运行中")
            
            for notebook in running_notebooks:
                with st.container():
                    col1, col2, col3 = st.columns([2, 1, 1])
                    
                    with col1:
                        st.write(f"**{notebook['notebook']}**")
                        st.caption(f"端口: {notebook['port']}")
                    
                    with col2:
                        # 访问按钮
                        if st.button(
                            "🌐", 
                            key=f"visit_{notebook['notebook']}",
                            help="在新标签页打开",
                            use_container_width=True
                        ):
                            st.markdown(
                                f'<a href="{notebook["url"]}" target="_blank">点击访问</a>',
                                unsafe_allow_html=True
                            )
                            st.success(f"请访问: {notebook['url']}")
                    
                    with col3:
                        # 停止按钮
                        if st.button(
                            "⏹️", 
                            key=f"stop_{notebook['notebook']}",
                            help="停止笔记本",
                            use_container_width=True
                        ):
                            self._stop_notebook(notebook['notebook'])
                    
                    st.markdown("---")
    
    def _render_quick_create(self):
        """渲染快速创建笔记本"""
        st.subheader("➕ 快速创建")
        
        # 预定义模板
        templates = {
            "空白笔记本": "blank_notebook.py",
            "策略回测": "strategy_backtest.py", 
            "个股分析": "stock_analysis.py",
            "市场研究": "market_research.py",
            "因子验证": "factor_validation.py"
        }
        
        selected_template = st.selectbox(
            "选择模板",
            options=list(templates.keys()),
            key="marimo_template_select"
        )
        
        notebook_name = st.text_input(
            "笔记本名称",
            value="",
            placeholder="输入笔记本名称",
            key="marimo_notebook_name"
        )
        
        if st.button("📝 创建并启动", key="marimo_create_launch"):
            if notebook_name:
                self._create_and_launch_notebook(notebook_name, selected_template)
            else:
                st.warning("请输入笔记本名称")
    
    def _render_manager_panel(self):
        """渲染管理面板"""
        st.header("🔬 Marimo 研究室管理")
        
        # 关闭按钮
        if st.button("❌ 关闭管理面板"):
            st.session_state.show_marimo_manager = False
            st.rerun()
        
        tab1, tab2, tab3 = st.tabs(["📚 笔记本管理", "🏃 运行状态", "⚙️ 设置"])
        
        with tab1:
            self._render_notebook_management()
        
        with tab2:
            self._render_running_status()
        
        with tab3:
            self._render_settings()
    
    def _render_notebook_management(self):
        """渲染笔记本管理"""
        st.subheader("📚 笔记本文件管理")
        
        notebooks = self.launcher.get_available_notebooks()
        
        if notebooks:
            # 笔记本列表
            for notebook in notebooks:
                with st.expander(f"📖 {notebook['name']}", expanded=False):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**文件信息**")
                        st.write(f"文件大小: {notebook['size']} 字节")
                        st.write(f"修改时间: {notebook['modified']}")
                        st.write(f"路径: {notebook['path']}")
                    
                    with col2:
                        st.write("**操作**")
                        
                        # 启动按钮
                        if st.button(f"🚀 启动", key=f"mgmt_launch_{notebook['name']}"):
                            self._launch_notebook(notebook['name'])
                        
                        # 编辑按钮（在系统编辑器中打开）
                        if st.button(f"✏️ 编辑", key=f"mgmt_edit_{notebook['name']}"):
                            try:
                                import webbrowser
                                webbrowser.open(f"file://{notebook['path']}")
                                st.success("已在系统编辑器中打开")
                            except Exception as e:
                                st.error(f"打开编辑器失败: {e}")
                        
                        # 删除按钮
                        if st.button(f"🗑️ 删除", key=f"mgmt_delete_{notebook['name']}"):
                            if st.session_state.get(f"confirm_delete_{notebook['name']}", False):
                                self._delete_notebook(notebook['name'])
                                st.session_state[f"confirm_delete_{notebook['name']}"] = False
                                st.rerun()
                            else:
                                st.session_state[f"confirm_delete_{notebook['name']}"] = True
                                st.warning("再次点击确认删除")
        else:
            st.info("暂无笔记本文件")
    
    def _render_running_status(self):
        """渲染运行状态"""
        st.subheader("🏃 运行状态监控")
        
        running_notebooks = self.launcher.list_running_notebooks()
        
        if running_notebooks:
            for notebook in running_notebooks:
                with st.container():
                    st.write(f"**{notebook['notebook']}**")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("端口", notebook['port'])
                    with col2:
                        runtime = datetime.now() - notebook['start_time']
                        st.metric("运行时间", f"{runtime.seconds // 60}分钟")
                    with col3:
                        st.metric("状态", "🟢 运行中")
                    
                    # 操作按钮
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if st.button(f"🌐 访问", key=f"status_visit_{notebook['notebook']}"):
                            st.markdown(
                                f'<a href="{notebook["url"]}" target="_blank">在新标签页打开</a>',
                                unsafe_allow_html=True
                            )
                    
                    with col2:
                        if st.button(f"🔄 重启", key=f"status_restart_{notebook['notebook']}"):
                            self._restart_notebook(notebook['notebook'])
                    
                    with col3:
                        if st.button(f"⏹️ 停止", key=f"status_stop_{notebook['notebook']}"):
                            self._stop_notebook(notebook['notebook'])
                    
                    st.markdown("---")
        else:
            st.info("暂无运行中的笔记本")
            st.markdown("💡 在左侧启动笔记本开始研究")
    
    def _render_settings(self):
        """渲染设置"""
        st.subheader("⚙️ Marimo 设置")
        
        # 端口设置
        st.write("**端口配置**")
        base_port = st.number_input(
            "起始端口",
            min_value=8000,
            max_value=9000,
            value=self.launcher.base_port,
            key="marimo_base_port"
        )
        
        max_port = st.number_input(
            "最大端口",
            min_value=base_port,
            max_value=9999,
            value=self.launcher.max_port,
            key="marimo_max_port"
        )
        
        if st.button("💾 保存端口设置"):
            self.launcher.base_port = base_port
            self.launcher.max_port = max_port
            st.success("端口设置已保存")
        
        # Marimo安装状态
        st.write("**系统状态**")
        marimo_installed = self.launcher.check_marimo_installed()
        
        if marimo_installed:
            st.success("✅ Marimo 已安装")
        else:
            st.error("❌ Marimo 未安装")
            st.code("pip install marimo")
        
        # 目录信息
        st.write("**目录信息**")
        st.write(f"笔记本目录: `{self.launcher.notebooks_dir}`")
        
        if st.button("📁 打开笔记本目录"):
            try:
                import webbrowser
                webbrowser.open(f"file://{self.launcher.notebooks_dir}")
                st.success("已打开笔记本目录")
            except Exception as e:
                st.error(f"打开目录失败: {e}")
    
    def _launch_notebook(self, notebook_name: str):
        """启动笔记本"""
        with st.spinner(f"正在启动 {notebook_name}..."):
            result = self.launcher.launch_notebook(notebook_name)
        
        if result['success']:
            st.success(f"✅ {result['message']}")
            st.info(f"🌐 访问地址: {result['url']}")
            
            # 更新session state
            st.session_state.marimo_running_notebooks[notebook_name] = {
                'port': result['port'],
                'url': result['url'],
                'start_time': datetime.now()
            }
            
            # 自动打开链接提示
            st.markdown(
                f'<a href="{result["url"]}" target="_blank">🚀 点击在新标签页打开</a>',
                unsafe_allow_html=True
            )
        else:
            st.error(f"❌ {result['message']}")
            
            # 如果是Marimo未安装，提供安装指导
            if "未安装" in result['message']:
                st.code("pip install marimo")
                st.info("安装完成后请刷新页面")
    
    def _stop_notebook(self, notebook_name: str):
        """停止笔记本"""
        with st.spinner(f"正在停止 {notebook_name}..."):
            result = self.launcher.stop_notebook(notebook_name)
        
        if result['success']:
            st.success(f"✅ {result['message']}")
            
            # 更新session state
            if notebook_name in st.session_state.marimo_running_notebooks:
                del st.session_state.marimo_running_notebooks[notebook_name]
        else:
            st.error(f"❌ {result['message']}")
    
    def _restart_notebook(self, notebook_name: str):
        """重启笔记本"""
        # 先停止
        stop_result = self.launcher.stop_notebook(notebook_name)
        if stop_result['success']:
            time.sleep(1)  # 等待一秒
            # 再启动
            self._launch_notebook(notebook_name)
        else:
            st.error(f"重启失败: {stop_result['message']}")
    
    def _create_and_launch_notebook(self, notebook_name: str, template: str):
        """创建并启动笔记本"""
        try:
            # 确保文件名以.py结尾
            if not notebook_name.endswith('.py'):
                notebook_name += '.py'
            
            notebook_path = self.launcher.notebooks_dir / notebook_name
            
            # 检查文件是否已存在
            if notebook_path.exists():
                st.warning(f"笔记本 {notebook_name} 已存在")
                return
            
            # 创建笔记本内容
            template_content = self._get_template_content(template)
            
            # 写入文件
            with open(notebook_path, 'w', encoding='utf-8') as f:
                f.write(template_content)
            
            st.success(f"✅ 笔记本 {notebook_name} 创建成功")
            
            # 启动笔记本
            self._launch_notebook(notebook_name)
            
        except Exception as e:
            st.error(f"创建笔记本失败: {e}")
            logger.error(f"创建笔记本失败: {e}")
    
    def _delete_notebook(self, notebook_name: str):
        """删除笔记本"""
        try:
            notebook_path = self.launcher.notebooks_dir / notebook_name
            
            if notebook_path.exists():
                notebook_path.unlink()
                st.success(f"✅ 笔记本 {notebook_name} 已删除")
            else:
                st.warning(f"笔记本 {notebook_name} 不存在")
                
        except Exception as e:
            st.error(f"删除笔记本失败: {e}")
            logger.error(f"删除笔记本失败: {e}")
    
    def _get_template_content(self, template: str) -> str:
        """获取模板内容"""
        templates = {
            "空白笔记本": '''"""
空白Marimo笔记本
"""
import marimo as mo

def __():
    mo.md("""
    # 📝 新建笔记本
    
    开始您的数据科学研究之旅！
    """)

if __name__ == "__main__":
    mo.run()
''',
            
            "策略回测": '''"""
策略回测笔记本
"""
import marimo as mo
import pandas as pd
import numpy as np
import plotly.graph_objects as go

def __():
    mo.md("""
    # 📈 策略回测分析
    
    用于测试和验证交易策略的性能。
    """)

def __():
    mo.md("## ⚙️ 策略参数")

def __():
    mo.md("## 📊 数据加载")

def __():
    mo.md("## 🎯 策略逻辑")

def __():
    mo.md("## 📈 回测结果")

if __name__ == "__main__":
    mo.run()
''',
            
            "个股分析": '''"""
个股深度分析笔记本
"""
import marimo as mo
import pandas as pd
import plotly.graph_objects as go

def __():
    mo.md("""
    # 🔍 个股深度分析
    
    对单只股票进行全面的技术和基本面分析。
    """)

def __():
    mo.md("## 📝 股票选择")

def __():
    mo.md("## 📊 技术分析")

def __():
    mo.md("## 💰 基本面分析")

def __():
    mo.md("## 🎯 投资建议")

if __name__ == "__main__":
    mo.run()
''',
            
            "市场研究": '''"""
市场研究笔记本
"""
import marimo as mo
import pandas as pd
import plotly.express as px

def __():
    mo.md("""
    # 📊 市场研究分析
    
    分析市场整体趋势和板块轮动。
    """)

def __():
    mo.md("## 🌍 市场概览")

def __():
    mo.md("## 🏭 板块分析")

def __():
    mo.md("## 📈 趋势分析")

if __name__ == "__main__":
    mo.run()
''',
            
            "因子验证": '''"""
因子验证笔记本
"""
import marimo as mo
import pandas as pd
import numpy as np

def __():
    mo.md("""
    # 🧪 因子验证分析
    
    验证和测试各种量化因子的有效性。
    """)

def __():
    mo.md("## 📊 因子构建")

def __():
    mo.md("## 🧪 因子测试")

def __():
    mo.md("## 📈 因子表现")

if __name__ == "__main__":
    mo.run()
'''
        }
        
        return templates.get(template, templates["空白笔记本"])
    
    def _refresh_notebook_status(self):
        """刷新笔记本状态"""
        st.session_state.marimo_last_refresh = datetime.now()
        # 清理已结束的进程
        self.launcher.list_running_notebooks()


# 便捷函数
def render_marimo_lab_sidebar():
    """在侧边栏渲染Marimo研究室"""
    lab = MarimoLabComponent()
    lab.render_sidebar()


def render_marimo_lab_main():
    """在主面板渲染Marimo管理界面"""
    lab = MarimoLabComponent()
    lab.render_main_panel()
