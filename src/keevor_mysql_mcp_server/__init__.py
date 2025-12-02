import os
import json
from contextlib import contextmanager
from typing import Any, Dict, List
import pymysql
from pymysql.connections import Connection
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("keevor-mysql-mcp-server")

# 连接池配置
_connection_pool: List[Connection] = []
_pool_size = 5


def _get_db_config() -> Dict[str, Any]:
    """获取数据库配置，带参数验证"""
    try:
        port = int(os.getenv("DB_PORT", "3306"))
    except ValueError:
        port = 3306
    
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": port,
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME", ""),
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": False,
    }


@contextmanager
def get_connection():
    """获取数据库连接的上下文管理器，确保连接正确关闭"""
    conn = None
    try:
        # 简单的连接池实现
        if _connection_pool:
            conn = _connection_pool.pop()
            try:
                conn.ping(reconnect=True)
            except:
                conn = pymysql.connect(**_get_db_config())
        else:
            conn = pymysql.connect(**_get_db_config())
        
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            try:
                # 归还连接到池中
                if len(_connection_pool) < _pool_size:
                    _connection_pool.append(conn)
                else:
                    conn.close()
            except:
                pass


def _format_error(error: Exception) -> str:
    """统一格式化错误信息"""
    return json.dumps({"error": str(error), "success": False}, ensure_ascii=False)


@mcp.tool()
def list_tables() -> str:
    """查询数据库中的所有表，包含表注释、引擎、行数等信息"""
    try:
        db_name = os.getenv("DB_NAME", "")
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT 
                        TABLE_NAME as name,
                        TABLE_COMMENT as comment,
                        ENGINE as engine,
                        TABLE_ROWS as `rows`,
                        CREATE_TIME as created,
                        UPDATE_TIME as updated
                    FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA = %s
                    ORDER BY TABLE_NAME
                    """,
                    (db_name,),
                )
                tables = cursor.fetchall()

        if not tables:
            return json.dumps({"tables": [], "count": 0, "success": True}, ensure_ascii=False)

        # 格式化日期时间
        for t in tables:
            if t.get("created"):
                t["created"] = str(t["created"])
            if t.get("updated"):
                t["updated"] = str(t["updated"])

        result = []
        for t in tables:
            line = f"- {t['name']}"
            if t["comment"]:
                line += f" ({t['comment']})"
            line += f" [引擎:{t['engine']}, 行数:{t['rows'] or 0}]"
            result.append(line)
        
        return "\n".join(result) + f"\n\n共 {len(tables)} 个表"
    except Exception as e:
        return _format_error(e)


@mcp.tool()
def describe_table(table_name: str) -> str:
    """查询指定表的详细结构，包含字段注释、索引、外键等信息

    Args:
        table_name: 要查询结构的表名
    """
    try:
        db_name = os.getenv("DB_NAME", "")
        result = []

        with get_connection() as conn:
            with conn.cursor() as cursor:
                # 获取表基本信息
                cursor.execute(
                    """
                    SELECT TABLE_COMMENT, ENGINE, TABLE_ROWS, CREATE_TIME, UPDATE_TIME
                    FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    """,
                    (db_name, table_name),
                )
                table_info = cursor.fetchone()

                if not table_info:
                    return json.dumps({"error": f"表 {table_name} 不存在", "success": False}, ensure_ascii=False)

                result.append(f"表名: {table_name}")
                result.append(f"注释: {table_info['TABLE_COMMENT'] or '无'}")
                result.append(f"引擎: {table_info['ENGINE']}")
                result.append(f"行数: {table_info['TABLE_ROWS'] or 0}")
                result.append("")

                # 获取字段信息
                cursor.execute(
                    """
                    SELECT 
                        COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY,
                        COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                    """,
                    (db_name, table_name),
                )
                columns = cursor.fetchall()

                result.append("字段列表:")
                for col in columns:
                    line = f"  - {col['COLUMN_NAME']} ({col['COLUMN_TYPE']})"
                    attrs = []
                    if col["COLUMN_KEY"] == "PRI":
                        attrs.append("主键")
                    elif col["COLUMN_KEY"] == "UNI":
                        attrs.append("唯一")
                    elif col["COLUMN_KEY"] == "MUL":
                        attrs.append("索引")
                    if col["IS_NULLABLE"] == "NO":
                        attrs.append("非空")
                    if col["EXTRA"]:
                        attrs.append(col["EXTRA"])
                    if attrs:
                        line += f" [{', '.join(attrs)}]"
                    if col["COLUMN_COMMENT"]:
                        line += f" - {col['COLUMN_COMMENT']}"
                    result.append(line)

                # 获取索引信息 - 使用参数化查询
                cursor.execute(
                    """
                    SELECT INDEX_NAME as Key_name, NON_UNIQUE as Non_unique, COLUMN_NAME as Column_name
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY INDEX_NAME, SEQ_IN_INDEX
                    """,
                    (db_name, table_name),
                )
                indexes = cursor.fetchall()
                if indexes:
                    result.append("")
                    result.append("索引:")
                    idx_map = {}
                    for idx in indexes:
                        name = idx["Key_name"]
                        if name not in idx_map:
                            idx_map[name] = {
                                "unique": not idx["Non_unique"],
                                "columns": [],
                            }
                        idx_map[name]["columns"].append(idx["Column_name"])
                    for name, info in idx_map.items():
                        unique = "唯一" if info["unique"] else "普通"
                        result.append(f"  - {name} ({unique}): {', '.join(info['columns'])}")

        return "\n".join(result)
    except Exception as e:
        return _format_error(e)


@mcp.tool()
def execute_sql(sql: str) -> str:
    """执行SQL语句

    Args:
        sql: 要执行的SQL语句
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)

                sql_upper = sql.strip().upper()
                if sql_upper.startswith("SELECT") or sql_upper.startswith("SHOW") or sql_upper.startswith("DESC"):
                    rows = cursor.fetchall()
                    if not rows:
                        return json.dumps({"data": [], "count": 0, "success": True}, ensure_ascii=False)
                    
                    # 格式化结果
                    formatted_rows = []
                    for row in rows:
                        formatted_row = {}
                        for key, value in row.items():
                            # 处理日期时间类型
                            if hasattr(value, 'isoformat'):
                                formatted_row[key] = value.isoformat()
                            else:
                                formatted_row[key] = value
                        formatted_rows.append(formatted_row)
                    
                    return json.dumps({"data": formatted_rows, "count": len(formatted_rows), "success": True}, ensure_ascii=False, indent=2)
                else:
                    conn.commit()
                    return json.dumps({"affected_rows": cursor.rowcount, "success": True}, ensure_ascii=False)
    except Exception as e:
        return _format_error(e)


def main():
    mcp.run()


if __name__ == "__main__":
    main()
