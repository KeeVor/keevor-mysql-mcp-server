import os
import pymysql
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("keevor-mysql-mcp-server")


def get_connection():
    """获取数据库连接"""
    return pymysql.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", ""),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


@mcp.tool()
def list_tables() -> str:
    """查询数据库中的所有表，包含表注释、引擎、行数等信息"""
    try:
        conn = get_connection()
        db_name = os.getenv("DB_NAME", "")
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
        conn.close()

        if not tables:
            return "没有找到任何表"

        result = []
        for t in tables:
            line = f"- {t['name']}"
            if t["comment"]:
                line += f" ({t['comment']})"
            line += f" [引擎:{t['engine']}, 行数:{t['rows'] or 0}]"
            result.append(line)
        return "\n".join(result)
    except Exception as e:
        return f"错误: {str(e)}"


@mcp.tool()
def describe_table(table_name: str) -> str:
    """查询指定表的详细结构，包含字段注释、索引、外键等信息

    Args:
        table_name: 要查询结构的表名
    """
    try:
        conn = get_connection()
        db_name = os.getenv("DB_NAME", "")
        result = []

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
                return f"表 {table_name} 不存在"

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

            # 获取索引信息
            cursor.execute(f"SHOW INDEX FROM `{table_name}`")
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

        conn.close()
        return "\n".join(result)
    except Exception as e:
        return f"错误: {str(e)}"


@mcp.tool()
def execute_sql(sql: str) -> str:
    """执行SQL语句

    Args:
        sql: 要执行的SQL语句
    """
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql)

            if sql.strip().upper().startswith("SELECT"):
                rows = cursor.fetchall()
                if not rows:
                    return "查询结果为空"
                return str(rows)
            else:
                conn.commit()
                return f"执行成功，影响行数: {cursor.rowcount}"
        conn.close()
    except Exception as e:
        return f"错误: {str(e)}"


def main():
    mcp.run()


if __name__ == "__main__":
    main()
