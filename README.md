# keevor-mysql-mcp-server

一个 MCP (Model Context Protocol) 服务器，用于连接 MySQL 数据库并执行常用操作。

## 功能

- `list_tables` - 查询数据库中的所有表
- `describe_table` - 查询指定表的结构
- `execute_sql` - 执行 SQL 语句

## 使用方法

### 1. 安装 uv

```bash
# Windows (PowerShell)
irm https://astral.sh/uv/install.ps1 | iex

# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. 配置 MCP

在 AI 工具的 MCP 配置文件中添加：

```json
{
  "mcpServers": {
    "keevor-mysql-mcp-server": {
      "command": "uvx",
      "args": ["keevor-mysql-mcp-server"],
      "env": {
        "DB_HOST": "localhost",
        "DB_PORT": "3306",
        "DB_USER": "root",
        "DB_PASSWORD": "your_password",
        "DB_NAME": "your_database"
      }
    }
  }
}
```

### 3. 开始使用

配置完成后，AI 助手即可使用以下工具：

- 查询所有表：调用 `list_tables`
- 查询表结构：调用 `describe_table`，传入表名
- 执行 SQL：调用 `execute_sql`，传入 SQL 语句

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| DB_HOST | 数据库主机地址 | localhost |
| DB_PORT | 数据库端口 | 3306 |
| DB_USER | 数据库用户名 | root |
| DB_PASSWORD | 数据库密码 | - |
| DB_NAME | 数据库名称 | - |

## License

MIT
