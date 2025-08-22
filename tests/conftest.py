# tests/conftest.py
import os
import sys

# 将项目根目录添加到 sys.path
# __file__ 指向 conftest.py 文件的路径
# os.path.dirname(__file__) 获取 tests 目录
# os.path.join(..., '..') 获取 tests 的上级目录，即项目根目录
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# 插入到 sys.path 的开头，优先查找项目内的模块
sys.path.insert(0, project_root)

print(f"Added {project_root} to sys.path")
