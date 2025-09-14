#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV文件导入SQLite数据库脚本
将已脱敏文件夹中的所有CSV文件导入到SQLite数据库中
"""

import os
import sqlite3
import pandas as pd
import glob
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_database():
    """创建数据库连接"""
    db_name = "客户数据.db"
    
    # 如果数据库已存在，删除它
    if os.path.exists(db_name):
        os.remove(db_name)
        logger.info(f"已删除现有数据库文件: {db_name}")
    
    # 创建新的数据库连接
    conn = sqlite3.connect(db_name)
    logger.info(f"已创建新数据库: {db_name}")
    
    return conn

def get_csv_files():
    """获取所有CSV文件"""
    csv_dir = "已脱敏"
    csv_pattern = os.path.join(csv_dir, "*.csv")
    csv_files = glob.glob(csv_pattern)
    
    logger.info(f"找到 {len(csv_files)} 个CSV文件")
    for file in csv_files:
        logger.info(f"  - {file}")
    
    return csv_files

def import_csv_to_db(conn, csv_files):
    """将CSV文件导入数据库"""
    all_dataframes = []
    
    for csv_file in csv_files:
        try:
            logger.info(f"正在读取文件: {csv_file}")
            
            # 读取CSV文件
            df = pd.read_csv(csv_file, encoding='utf-8')
            
            logger.info(f"  - 文件大小: {len(df)} 行, {len(df.columns)} 列")
            logger.info(f"  - 列名: {list(df.columns)}")
            
            # 添加到列表中
            all_dataframes.append(df)
            
        except Exception as e:
            logger.error(f"读取文件 {csv_file} 时出错: {e}")
            continue
    
    if not all_dataframes:
        logger.error("没有成功读取任何CSV文件")
        return False
    
    try:
        # 合并所有数据
        logger.info("正在合并所有数据...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        logger.info(f"合并后数据大小: {len(combined_df)} 行, {len(combined_df.columns)} 列")
        
        # 创建表并导入数据
        table_name = "customer_data"
        logger.info(f"正在创建表 {table_name} 并导入数据...")
        
        combined_df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        logger.info(f"数据导入成功！")
        logger.info(f"  - 表名: {table_name}")
        logger.info(f"  - 总行数: {len(combined_df)}")
        logger.info(f"  - 总列数: {len(combined_df.columns)}")
        
        return True
        
    except Exception as e:
        logger.error(f"导入数据时出错: {e}")
        return False

def verify_database(conn):
    """验证数据库内容"""
    try:
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        logger.info(f"数据库中的表: {[table[0] for table in tables]}")
        
        # 检查数据量
        cursor.execute("SELECT COUNT(*) FROM customer_data")
        count = cursor.fetchone()[0]
        logger.info(f"customer_data表中的记录数: {count}")
        
        # 检查列信息
        cursor.execute("PRAGMA table_info(customer_data)")
        columns = cursor.fetchall()
        logger.info(f"customer_data表的列数: {len(columns)}")
        
        # 显示前几列的信息
        logger.info("前10列信息:")
        for i, col in enumerate(columns[:10]):
            logger.info(f"  {i+1}. {col[1]} ({col[2]})")
        
        if len(columns) > 10:
            logger.info(f"  ... 还有 {len(columns) - 10} 列")
        
        # 检查日期范围
        cursor.execute("SELECT MIN(stat_date), MAX(stat_date) FROM customer_data WHERE stat_date IS NOT NULL")
        date_range = cursor.fetchone()
        if date_range[0] and date_range[1]:
            logger.info(f"日期范围: {date_range[0]} 到 {date_range[1]}")
        
        # 检查大区数据
        cursor.execute("SELECT DISTINCT gccxbigzone_name FROM customer_data WHERE gccxbigzone_name IS NOT NULL LIMIT 10")
        zones = cursor.fetchall()
        logger.info(f"大区名称示例: {[zone[0] for zone in zones]}")
        
        return True
        
    except Exception as e:
        logger.error(f"验证数据库时出错: {e}")
        return False

def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("开始CSV文件导入SQLite数据库")
    logger.info("=" * 60)
    
    start_time = datetime.now()
    
    try:
        # 1. 创建数据库
        conn = create_database()
        
        # 2. 获取CSV文件
        csv_files = get_csv_files()
        if not csv_files:
            logger.error("没有找到CSV文件")
            return False
        
        # 3. 导入数据
        success = import_csv_to_db(conn, csv_files)
        if not success:
            logger.error("数据导入失败")
            return False
        
        # 4. 验证数据库
        verify_success = verify_database(conn)
        if not verify_success:
            logger.error("数据库验证失败")
            return False
        
        # 5. 提交并关闭连接
        conn.commit()
        conn.close()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 60)
        logger.info("CSV文件导入完成！")
        logger.info(f"总耗时: {duration}")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"程序执行出错: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ 数据库创建成功！")
        print("📁 数据库文件: 客户数据.db")
        print("🔍 可以使用以下方式查看数据:")
        print("   - 使用SQLite客户端工具")
        print("   - 使用Python脚本查询")
        print("   - 使用Flask API接口")
    else:
        print("\n❌ 数据库创建失败！")
        print("请检查错误信息并重试")
