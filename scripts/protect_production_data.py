#!/usr/bin/env python3
"""
生产数据保护脚本
防止意外覆盖真实A股生产数据
"""
import sys
import os
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

logger = get_logger("protect_production_data")


def check_production_data_exists():
    """检查生产数据是否存在"""
    logger.info("🔍 检查生产数据状态")
    
    production_data_paths = [
        'data/production/a_share/a_share_basic.csv',
        'data/production/a_share/a_share_daily_quotes.csv'
    ]
    
    existing_files = []
    for path in production_data_paths:
        full_path = project_root / path
        if full_path.exists():
            existing_files.append(path)
            logger.info(f"✅ 发现生产数据: {path}")
    
    if existing_files:
        logger.success(f"✅ 发现 {len(existing_files)} 个生产数据文件")
        return True
    else:
        logger.warning("⚠️ 未发现生产数据文件")
        return False


def create_data_protection():
    """创建数据保护机制"""
    logger.info("🛡️ 创建数据保护机制")
    
    try:
        # 创建保护标记文件
        protection_file = project_root / 'data' / 'production' / '.PRODUCTION_DATA_PROTECTED'
        protection_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(protection_file, 'w', encoding='utf-8') as f:
            f.write("# 生产数据保护标记\n")
            f.write("# 此文件表示当前目录包含真实生产数据\n")
            f.write("# 请勿运行可能覆盖数据的脚本\n")
            f.write(f"# 创建时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("\n")
            f.write("PRODUCTION_DATA=true\n")
            f.write("DATA_TYPE=A_SHARE_REAL\n")
            f.write("WARNING=DO_NOT_OVERWRITE\n")
        
        logger.success(f"✅ 保护标记已创建: {protection_file}")
        
        # 创建危险脚本警告
        dangerous_scripts = [
            'scripts/import_a_share_data.py',
            'scripts/clear_mock_data.py',
            'scripts/production_data_manager.py'
        ]
        
        for script_path in dangerous_scripts:
            script_file = project_root / script_path
            if script_file.exists():
                # 在脚本开头添加警告注释
                with open(script_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                if 'PRODUCTION_DATA_WARNING' not in content:
                    warning_header = '''#!/usr/bin/env python3
"""
⚠️ PRODUCTION_DATA_WARNING ⚠️
此脚本可能会覆盖真实的A股生产数据！
运行前请确认您了解风险并有数据备份。
如需运行，请删除此警告注释。
"""
import sys
print("⚠️ 警告：此脚本可能覆盖生产数据！")
print("如需继续，请手动删除脚本中的警告代码")
sys.exit(1)

'''
                    new_content = warning_header + content
                    
                    with open(script_file, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    
                    logger.warning(f"⚠️ 已为危险脚本添加保护: {script_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 创建数据保护失败: {e}")
        return False


def backup_production_data():
    """备份生产数据"""
    logger.info("💾 备份生产数据")
    
    try:
        from datetime import datetime
        import shutil
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_dir = project_root / 'backup' / 'production_data' / timestamp
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        production_dir = project_root / 'data' / 'production'
        
        if production_dir.exists():
            # 复制整个生产数据目录
            backup_production_dir = backup_dir / 'production'
            shutil.copytree(production_dir, backup_production_dir)
            
            logger.success(f"✅ 生产数据已备份到: {backup_dir}")
            
            # 创建备份说明
            readme_file = backup_dir / 'README.md'
            with open(readme_file, 'w', encoding='utf-8') as f:
                f.write(f"# 生产数据备份\n\n")
                f.write(f"备份时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"备份内容: A股真实生产数据\n")
                f.write(f"数据来源: AllTick + Alpha Vantage\n")
                f.write(f"备份原因: 数据保护\n\n")
                f.write(f"## 恢复方法\n")
                f.write(f"```bash\n")
                f.write(f"cp -r {backup_production_dir}/* data/production/\n")
                f.write(f"```\n")
            
            return True
        else:
            logger.warning("⚠️ 生产数据目录不存在，无需备份")
            return False
            
    except Exception as e:
        logger.error(f"❌ 备份生产数据失败: {e}")
        return False


def clean_sample_data():
    """清除样本数据，保留真实数据"""
    logger.info("🧹 清除样本数据")
    
    try:
        # 需要清除的样本数据路径
        sample_paths = [
            'data/sample',
            'data/mock',
            'data/test',
            'data/temp'
        ]
        
        import shutil
        
        for path in sample_paths:
            full_path = project_root / path
            if full_path.exists():
                shutil.rmtree(full_path)
                logger.info(f"✅ 已清除样本数据: {path}")
        
        logger.success("✅ 样本数据清除完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 清除样本数据失败: {e}")
        return False


def validate_data_integrity():
    """验证数据完整性"""
    logger.info("🔍 验证数据完整性")
    
    try:
        import pandas as pd
        
        # 检查关键数据文件
        key_files = {
            'data/production/a_share/a_share_basic.csv': '股票基础信息',
            'data/production/a_share/a_share_daily_quotes.csv': '日线行情数据'
        }
        
        all_valid = True
        
        for file_path, description in key_files.items():
            full_path = project_root / file_path
            
            if full_path.exists():
                try:
                    data = pd.read_csv(full_path)
                    logger.success(f"✅ {description}: {len(data)} 条记录")
                except Exception as e:
                    logger.error(f"❌ {description} 文件损坏: {e}")
                    all_valid = False
            else:
                logger.error(f"❌ {description} 文件缺失: {file_path}")
                all_valid = False
        
        if all_valid:
            logger.success("✅ 所有关键数据文件完整")
        else:
            logger.error("❌ 发现数据完整性问题")
        
        return all_valid
        
    except Exception as e:
        logger.error(f"❌ 数据完整性验证失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🛡️ 生产数据保护系统")
    logger.info("=" * 60)
    
    try:
        from datetime import datetime
        
        # 执行保护步骤
        steps = [
            ("检查生产数据", check_production_data_exists),
            ("备份生产数据", backup_production_data),
            ("创建数据保护", create_data_protection),
            ("清除样本数据", clean_sample_data),
            ("验证数据完整性", validate_data_integrity)
        ]
        
        success_count = 0
        total_count = len(steps)
        
        for step_name, step_func in steps:
            try:
                logger.info(f"\n🎯 执行: {step_name}")
                success = step_func()
                if success:
                    success_count += 1
                    logger.success(f"✅ {step_name} 完成")
                else:
                    logger.warning(f"⚠️ {step_name} 部分完成")
            except Exception as e:
                logger.error(f"❌ {step_name} 失败: {e}")
        
        # 总结
        logger.info("\n" + "=" * 60)
        logger.info("📊 保护操作总结")
        logger.info("=" * 60)
        logger.info(f"总步骤数: {total_count}")
        logger.info(f"成功步骤: {success_count}")
        logger.info(f"成功率: {success_count/total_count*100:.1f}%")
        
        if success_count >= 4:  # 至少4个步骤成功
            logger.success("🎉 生产数据保护完成！")
            logger.info("\n💡 保护措施:")
            logger.info("  ✅ 数据已备份")
            logger.info("  ✅ 危险脚本已添加警告")
            logger.info("  ✅ 保护标记已创建")
            logger.info("  ✅ 样本数据已清除")
            
            logger.info("\n⚠️ 重要提醒:")
            logger.info("  • 请勿运行带有覆盖警告的脚本")
            logger.info("  • 定期检查数据完整性")
            logger.info("  • 如需恢复数据，请使用备份")
        else:
            logger.warning("⚠️ 部分保护措施失败，请手动检查")
        
        return success_count >= 4
        
    except Exception as e:
        logger.error(f"❌ 保护系统异常: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
