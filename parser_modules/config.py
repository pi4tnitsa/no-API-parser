"""
Configuration module
Handles loading and management of configuration settings
"""

import csv
import json
import logging
import os
import yaml
from typing import Dict, List, Any, Optional

logger = logging.getLogger("telegram_parser.config")

class Config:
    """Configuration manager for the Telegram parser."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration.
        
        Args:
            config_path: Path to the YAML config file
        """
        # Default configuration
        self.phone = None
        self.headless = False
        self.proxy = None
        self.channels = []
        self.limit = 100
        self.output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
        self.export_format = "xlsx"
        
        # Load configuration from file if provided
        if config_path and os.path.exists(config_path):
            self._load_config_file(config_path)
            
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
    def _load_config_file(self, config_path: str) -> None:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
                
            if not config_data:
                logger.warning(f"Config file {config_path} is empty or invalid")
                return
                
            # Load settings
            if 'auth' in config_data:
                auth_config = config_data['auth']
                self.phone = auth_config.get('phone', self.phone)
                self.headless = auth_config.get('headless', self.headless)
                self.proxy = auth_config.get('proxy', self.proxy)
                
            if 'parser' in config_data:
                parser_config = config_data['parser']
                self.limit = parser_config.get('limit', self.limit)
                
            if 'channels' in config_data and isinstance(config_data['channels'], list):
                self.channels = config_data['channels']
                
            if 'output' in config_data:
                output_config = config_data['output']
                output_dir = output_config.get('directory')
                if output_dir:
                    self.output_dir = os.path.abspath(output_dir)
                self.export_format = output_config.get('format', self.export_format)
                
            logger.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logger.error(f"Error loading config file {config_path}: {str(e)}")
            
    def update_from_args(self, args) -> None:
        """Update configuration from command line arguments."""
        # Update settings from args, overriding config file
        if hasattr(args, 'phone') and args.phone:
            self.phone = args.phone
            
        if hasattr(args, 'headless'):
            self.headless = args.headless
            
        if hasattr(args, 'proxy') and args.proxy:
            self.proxy = args.proxy
            
        if hasattr(args, 'limit') and args.limit:
            self.limit = args.limit
            
        if hasattr(args, 'output') and args.output:
            self.output_dir = os.path.abspath(args.output)
            os.makedirs(self.output_dir, exist_ok=True)
            
        if hasattr(args, 'format') and args.format:
            self.export_format = args.format
            
    def load_channels_from_file(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Load channels from a file (JSON or CSV).
        
        Args:
            file_path: Path to the input file
            
        Returns:
            List of channel information dictionaries
        """
        if not os.path.exists(file_path):
            logger.error(f"Channel file not found: {file_path}")
            return []
            
        try:
            # Determine file type by extension
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext == '.json':
                return self._load_channels_from_json(file_path)
            elif file_ext == '.csv':
                return self._load_channels_from_csv(file_path)
            else:
                logger.error(f"Unsupported channel file format: {file_ext}")
                return []
        except Exception as e:
            logger.error(f"Error loading channels from {file_path}: {str(e)}")
            return []
            
    def _load_channels_from_json(self, file_path: str) -> List[Dict[str, Any]]:
        """Load channels from JSON file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if isinstance(data, list):
            channels = data
        elif isinstance(data, dict) and 'channels' in data:
            channels = data['channels']
        else:
            logger.warning("JSON file does not contain a channels list")
            channels = []
            
        # Validate channels
        valid_channels = []
        for channel in channels:
            if not isinstance(channel, dict):
                logger.warning(f"Skipping invalid channel entry: {channel}")
                continue
                
            # Check if at least one identifier is present
            if not any(key in channel for key in ['name', 'username', 'id', 'url']):
                logger.warning(f"Channel missing identifier (name/username/id/url): {channel}")
                continue
                
            valid_channels.append(channel)
            
        logger.info(f"Loaded {len(valid_channels)} channels from {file_path}")
        return valid_channels
        
    def _load_channels_from_csv(self, file_path: str) -> List[Dict[str, Any]]:
        """Load channels from CSV file."""
        channels = []
        
        with open(file_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Clean up empty values
                channel = {k: v for k, v in row.items() if v.strip()}
                
                # Check if at least one identifier is present
                if not any(key in channel for key in ['name', 'username', 'id', 'url']):
                    logger.warning(f"Channel missing identifier (name/username/id/url): {channel}")
                    continue
                    
                channels.append(channel)
                
        logger.info(f"Loaded {len(channels)} channels from {file_path}")
        return channels
        
    def __str__(self) -> str:
        """Get string representation of configuration."""
        return (
            f"Config(phone={'*' * len(self.phone) if self.phone else None}, "
            f"headless={self.headless}, "
            f"proxy={self.proxy}, "
            f"channels_count={len(self.channels)}, "
            f"limit={self.limit}, "
            f"output_dir={self.output_dir}, "
            f"export_format={self.export_format})"
        )