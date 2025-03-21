#!/usr/bin/env python3
"""
Telegram Channel Parser
Main module for handling command-line arguments and orchestrating the parsing process.
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime

from parser_modules.auth import TelegramAuth
from parser_modules.channel_parser import ChannelParser
from parser_modules.data_exporter import DataExporter
from parser_modules.config import Config

# Set up logging
def setup_logging():
    """Configure logging for the application."""
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f"telegram_parser_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger("telegram_parser")

logger = setup_logging()

async def main():
    """Main entry point for the Telegram parser application."""
    parser = argparse.ArgumentParser(description="Telegram Channel Parser")
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to config file")
    parser.add_argument("--phone", type=str, help="Telegram phone number")
    parser.add_argument("--channels", type=str, help="Path to JSON/CSV file with channels to parse")
    parser.add_argument("--channel", type=str, help="Single channel to parse (username or ID)")
    parser.add_argument("--output", type=str, default="output", help="Output directory")
    parser.add_argument("--format", choices=["json", "csv", "xlsx"], default="xlsx", help="Output format")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of posts to parse")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--proxy", type=str, help="Proxy URL (e.g., socks5://user:pass@host:port)")
    
    args = parser.parse_args()
    
    # Load config
    config = Config(args.config)
    config.update_from_args(args)
    
    logger.info(f"Starting Telegram parser with config: {config}")
    
    # Initialize telegram auth
    auth = TelegramAuth(config)
    
    try:
        # Launch browser and login
        logger.info("Launching browser and logging in to Telegram Web")
        browser, page = await auth.login()
        
        # Initialize channel parser
        channel_parser = ChannelParser(browser, page, config)
        
        # Initialize data exporter
        data_exporter = DataExporter(config)
        
        # Parse channels
        channels = []
        if args.channels:
            channels = config.load_channels_from_file(args.channels)
        elif args.channel:
            channels = [{"name": args.channel}]
        else:
            channels = config.channels
            
        if not channels:
            logger.error("No channels specified for parsing")
            await browser.close()
            return
            
        logger.info(f"Found {len(channels)} channels to parse")
        
        for channel_info in channels:
            channel_name = channel_info.get("name") or channel_info.get("username") or channel_info.get("id")
            
            if not channel_name:
                logger.warning(f"Skipping channel with missing identifier: {channel_info}")
                continue
                
            logger.info(f"Parsing channel: {channel_name}")
            
            try:
                channel_data = await channel_parser.parse_channel(channel_name)
                
                if channel_data and channel_data["posts"]:
                    output_file = data_exporter.export_data(channel_data, channel_name)
                    logger.info(f"Exported {len(channel_data['posts'])} posts to {output_file}")
                else:
                    logger.warning(f"No posts found for channel: {channel_name}")
            except Exception as e:
                logger.error(f"Error parsing channel {channel_name}: {str(e)}", exc_info=True)
                
        logger.info("Parsing completed")
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
    finally:
        # Close browser
        try:
            await browser.close()
            logger.info("Browser closed")
        except Exception:
            pass

if __name__ == "__main__":
    asyncio.run(main())