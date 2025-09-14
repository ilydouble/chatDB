#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flask API接口 - 超强健版本（处理所有JSON格式问题）
"""

from flask import Flask, request, jsonify
import sqlite3
import pandas as pd
import os
from datetime import datetime
import traceback
import logging
import json
import re

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 配置数据库连接池
DATABASE = "客户数据.db"

def get_db_connection():
    """
    获取数据库连接
    """
    try:
        conn = sqlite3.connect(DATABASE, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        raise Exception(f"数据库连接失败: {e}")

def execute_sql_query(sql_query):
    """
    执行SQL查询并返回结果
    """
    conn = None
    try:
        logger.info(f"执行SQL查询: {sql_query[:100]}...")
        
        # 检查数据库文件是否存在
        if not os.path.exists(DATABASE):
            raise Exception(f"数据库文件不存在: {DATABASE}")
        
        # 连接数据库
        conn = get_db_connection()
        
        # 执行查询
        df = pd.read_sql_query(sql_query, conn)
        
        # 转换DataFrame为字典列表
        data = df.to_dict('records')
        
        # 如果查询结果为空但有列名，创建一行空数据
        if df.empty and len(df.columns) > 0:
            empty_row = {col: None for col in df.columns}
            data = [empty_row]
            logger.info(f"查询结果为空，创建空行数据，列数: {len(df.columns)}")
            logger.info(f"空行数据: {empty_row}")
        else:
            logger.info(f"查询成功，返回 {len(data)} 条记录")
        
        return {
            "success": True,
            "error": None,
            "data": data,
            "row_count": len(data),
            "columns": list(df.columns)
        }
        
    except Exception as e:
        logger.error(f"查询失败: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        return {
            "success": False,
            "error": str(e),
            "data": None
        }
    finally:
        if conn:
            conn.close()

def fix_json_format(raw_data):
    """
    修复JSON格式问题
    """
    try:
        # 方法1: 直接解析
        return json.loads(raw_data)
    except:
        pass
    
    try:
        # 方法2: 修复换行符问题 - 更智能的处理
        # 找到sql字段的值，并正确处理其中的换行符
        sql_pattern = r'"sql":\s*"([^"]*(?:\\.[^"]*)*)"'
        sql_match = re.search(sql_pattern, raw_data, re.DOTALL)
        
        if sql_match:
            sql_content = sql_match.group(1)
            # 将未转义的换行符转换为转义的换行符
            sql_content = sql_content.replace('\n', '\\n').replace('\r', '\\r')
            # 替换原始数据中的SQL内容
            fixed_data = raw_data.replace(sql_match.group(0), f'"sql": "{sql_content}"')
            return json.loads(fixed_data)
    except:
        pass
    
    try:
        # 方法3: 更激进的修复 - 处理所有字符串值中的换行符
        # 使用正则表达式找到所有字符串值并修复换行符
        def fix_string_value(match):
            key = match.group(1)
            value = match.group(2)
            # 修复值中的换行符
            fixed_value = value.replace('\n', '\\n').replace('\r', '\\r')
            return f'"{key}": "{fixed_value}"'
        
        # 匹配 "key": "value" 格式，其中value可能包含换行符
        pattern = r'"([^"]+)":\s*"([^"]*(?:\n[^"]*)*)"'
        fixed_data = re.sub(pattern, fix_string_value, raw_data, flags=re.DOTALL)
        return json.loads(fixed_data)
    except:
        pass
    
    try:
        # 方法4: 手动构建JSON - 提取SQL语句
        sql_match = re.search(r'"sql":\s*"([^"]*(?:\\.[^"]*)*)"', raw_data, re.DOTALL)
        if sql_match:
            sql_content = sql_match.group(1)
            # 清理SQL内容
            sql_content = sql_content.replace('\\n', '\n').replace('\\r', '\r')
            return {"sql": sql_content}
    except:
        pass
    
    try:
        # 方法5: 使用ast.literal_eval作为最后手段
        import ast
        # 尝试将字符串转换为Python字典
        data_dict = ast.literal_eval(raw_data)
        return data_dict
    except:
        pass
    
    raise Exception("无法解析JSON数据")

@app.route('/')
def home():
    """
    API首页
    """
    return jsonify({
        "message": "SQL查询API服务",
        "version": "4.0.0",
        "endpoints": {
            "/": "API信息",
            "/query": "执行SQL查询",
            "/health": "健康检查"
        }
    })

@app.route('/health')
def health_check():
    """
    健康检查接口
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM customer_data")
        count = cursor.fetchone()[0]
        conn.close()
        
        return jsonify({
            "status": "healthy",
            "database": "connected",
            "total_records": count,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"健康检查失败: {e}")
        return jsonify({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/query', methods=['POST'])
def query_sql():
    """
    执行SQL查询接口
    """
    try:
        logger.info(f"收到POST请求")
        
        # 获取原始请求数据
        raw_data = request.get_data(as_text=True)
        logger.info(f"原始请求数据: {raw_data[:200]}...")
        
        # 尝试解析JSON
        try:
            data = fix_json_format(raw_data)
            logger.info("JSON解析成功")
        except Exception as e:
            logger.error(f"JSON解析失败: {e}")
            return jsonify({
                "success": False,
                "error": f"JSON格式错误: {str(e)}",
                "data": None
            }), 400
        
        if not data:
            logger.warning("请求体为空")
            return jsonify({
                "success": False,
                "error": "请求体不能为空",
                "data": None
            }), 400
        
        # 获取SQL查询语句
        sql_query = data.get('sql')
        print(sql_query)
        if not sql_query:
            logger.warning("缺少sql参数")
            return jsonify({
                "success": False,
                "error": "缺少sql参数",
                "data": None
            }), 400
        
        
        # 执行查询
        result = execute_sql_query(sql_query)
        
        if result["success"]:
            return jsonify({
                "success": True,
                "error": None,
                "data": result["data"],
                "row_count": result["row_count"],
                "columns": result["columns"],
                "sql": sql_query,
                "timestamp": datetime.now().isoformat()
            })
        else:
            return jsonify({
                "success": False,
                "error": result["error"],
                "data": None,
                "sql": sql_query,
                "timestamp": datetime.now().isoformat()
            }), 400
            
    except Exception as e:
        logger.error(f"服务器异常: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        return jsonify({
            "success": False,
            "error": f"服务器错误: {str(e)}",
            "data": None,
            "timestamp": datetime.now().isoformat()
        }), 500

@app.errorhandler(404)
def not_found(error):
    """
    404错误处理
    """
    return jsonify({
        "success": False,
        "error": "接口不存在",
        "data": None
    }), 404

@app.errorhandler(405)
def method_not_allowed(error):
    """
    405错误处理
    """
    return jsonify({
        "success": False,
        "error": "请求方法不允许",
        "data": None
    }), 405

if __name__ == '__main__':
    print("🚀 启动Flask API服务（超强健版本）...")
    print("📊 数据库文件: 客户数据.db")
    print("🌐 API地址: http://localhost:5000")
    print("📖 API文档:")
    print("  GET  /health - 健康检查")
    print("  POST /query  - 执行SQL查询")
    print("=" * 50)
    
    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True)
