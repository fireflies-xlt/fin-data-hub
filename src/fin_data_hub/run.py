#!/usr/bin/env python
"""
金融数据中心API服务启动脚本
支持通过命令行参数或环境变量配置
"""
import argparse
import logging
import sys

import uvicorn


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="启动金融数据中心API服务")

    # 服务器配置
    parser.add_argument("--host", default="0.0.0.0", help="监听地址")
    parser.add_argument("--port", type=int, default=8000, help="监听端口")
    parser.add_argument("--workers", type=int, default=1, help="工作进程数")
    parser.add_argument(
        "--reload",
        action="store_true",
        default=False,
        help="是否自动重载",
    )

    # 日志配置
    parser.add_argument(
        "--log-config",
        default="logging.yaml",
        help="日志配置文件路径，支持YAML或JSON格式",
    )
    return parser.parse_args()


def start_server():
    """启动服务器函数，可以被外部调用"""
    args = parse_args()

    # 基本日志配置（仅用于启动过程，随后将被配置文件覆盖）
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # 日志输出启动信息
    logger = logging.getLogger(__name__)
    logger.info(f"启动金融数据中心API服务")
    logger.info(
        f"服务器配置: host={args.host}, port={args.port}, workers={args.workers}, reload={args.reload}"
    )
    logger.info(f"使用日志配置文件: {args.log_config}")

    # 多进程提示
    if args.workers > 1:
        logger.info(f"多进程模式: {args.workers}个工作进程将被启动")

    try:
        # 启动Uvicorn
        uvicorn.run(
            "fin_data_hub.main:app",
            host=args.host,
            port=args.port,
            workers=args.workers,
            reload=args.reload,
            log_config=args.log_config,
        )
    except Exception as e:
        logger.critical(f"启动服务时发生严重错误: {e}", exc_info=True)
        sys.exit(1)


def main():
    """命令行入口点"""
    start_server()


if __name__ == "__main__":
    main()
