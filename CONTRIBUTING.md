# 贡献指南

欢迎为 Vespera 项目做贡献！在提交代码之前，请花些时间阅读以下指南。

## 开发环境设置

1. 克隆仓库：
   ```bash
   git clone https://github.com/yourusername/vespera.git
   cd vespera
   ```

2. 创建并激活虚拟环境：
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # 或
   .\venv\Scripts\activate  # Windows
   ```

3. 安装开发依赖：
   ```bash
   pip install -e ".[dev,test,docs]"
   ```

4. 安装 pre-commit 钩子：
   ```bash
   pre-commit install
   ```

## 代码风格

- 使用 [Black](https://black.readthedocs.io/) 格式化代码
- 使用 [isort](https://pycqa.github.io/isort/) 排序导入
- 遵循 [PEP 8](https://www.python.org/dev/peps/pep-0008/) 代码风格
- 使用类型注解（Type Hints）

## 提交信息

使用 [Conventional Commits](https://www.conventionalcommits.org/) 规范：

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### 提交类型

- **feat**: 新功能
- **fix**: Bug 修复
- **docs**: 文档更新
- **style**: 代码格式（不影响代码运行的变动）
- **refactor**: 重构（既不增加新功能，也不是修复bug）
- **perf**: 性能优化
- **test**: 增加测试
- **chore**: 构建过程或辅助工具的变动

## 开发流程

1. 从 `main` 分支创建新分支：
   ```bash
   git checkout -b feat/your-feature-name
   ```

2. 进行更改并运行测试：
   ```bash
   pytest
   ```

3. 确保代码通过所有检查：
   ```bash
   pre-commit run --all-files
   ```

4. 提交更改：
   ```bash
   git add .
   git commit -m "feat: 添加新功能"
   ```

5. 推送到远程仓库：
   ```bash
   git push -u origin feat/your-feature-name
   ```

6. 创建 Pull Request

## 测试

- 编写测试覆盖新功能和修复的 bug
- 确保测试通过：`pytest`
- 检查测试覆盖率：`pytest --cov=src`

## 文档

- 更新相关文档
- 确保文档构建通过：`cd docs && make html`

## 问题报告

- 使用 GitHub Issues 报告问题
- 提供详细的复现步骤
- 包括相关日志和截图（如果适用）

## 行为准则

请遵循我们的 [行为准则](CODE_OF_CONDUCT.md)。

## 许可证

通过提交贡献，您同意您的贡献将根据 [MIT 许可证](LICENSE) 进行许可。
