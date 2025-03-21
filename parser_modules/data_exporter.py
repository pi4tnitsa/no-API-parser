"""
Data exporter module
Handles exporting parsed data to various formats
"""

import csv
import json
import logging
import os
import re
from datetime import datetime
from typing import Dict, Any, List, Optional

import pandas as pd

logger = logging.getLogger("telegram_parser.data_exporter")

class DataExporter:
    """Exports parsed data to various formats."""

    def _sanitize_filename(self, filename: str) -> str:
        """Удаляет недопустимые символы из имени файла."""
        return re.sub(r'[\/:*?"<>|]', '_', filename)
    
    def __init__(self, config):
        """Initialize with config."""
        self.config = config
        self.output_dir = config.output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
    def export_data(self, data: Dict[str, Any], channel_name: str) -> str:
        """
        Export data to the specified format.
        
        Args:
            data: The data to export
            channel_name: Name of the channel
            
        Returns:
            Path to the exported file
        """
        # Sanitize channel name for filename
        safe_channel_name = self._sanitize_filename(channel_name)
        
        # Create timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Determine export format
        export_format = self.config.export_format.lower()
        
        if export_format == "json":
            return self._export_json(data, safe_channel_name, timestamp)
        elif export_format == "csv":
            return self._export_csv(data, safe_channel_name, timestamp)
        elif export_format == "xlsx":
            return self._export_xlsx(data, safe_channel_name, timestamp)
        else:
            logger.warning(f"Unknown export format: {export_format}, defaulting to JSON")
            return self._export_json(data, safe_channel_name, timestamp)
    
    def _export_json(self, data: Dict[str, Any], channel_name: str, timestamp: str) -> str:
        """Export data to JSON format."""
        output_file = os.path.join(self.output_dir, f"{channel_name}_{timestamp}.json")
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Data exported to JSON: {output_file}")
            return output_file
        except Exception as e:
            logger.error(f"Error exporting to JSON: {str(e)}")
            # Try to save with a simple filename as fallback
            fallback_file = os.path.join(self.output_dir, f"export_{timestamp}.json")
            with open(fallback_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return fallback_file
    
    def _export_csv(self, data: Dict[str, Any], channel_name: str, timestamp: str) -> str:
        """Export data to CSV format."""
        output_file = os.path.join(self.output_dir, f"{channel_name}_{timestamp}.csv")
        
        try:
            # Extract posts data
            posts = data.get("posts", [])
            
            if not posts:
                logger.warning("No posts to export")
                return self._export_json(data, channel_name, timestamp)  # Fallback to JSON
                
            # Normalize data for CSV
            flat_posts = []
            for post in posts:
                flat_post = self._flatten_post(post)
                flat_posts.append(flat_post)
                
            # Write to CSV
            with open(output_file, 'w', encoding='utf-8', newline='') as f:
                if flat_posts:
                    fieldnames = flat_posts[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(flat_posts)
                    
            logger.info(f"Data exported to CSV: {output_file}")
            
            # Also export channel info
            channel_info = data.get("channel", {})
            if channel_info:
                channel_info_file = os.path.join(self.output_dir, f"{channel_name}_info_{timestamp}.json")
                with open(channel_info_file, 'w', encoding='utf-8') as f:
                    json.dump(channel_info, f, ensure_ascii=False, indent=2)
                logger.info(f"Channel info exported to: {channel_info_file}")
                
            return output_file
        except Exception as e:
            logger.error(f"Error exporting to CSV: {str(e)}")
            # Fallback to JSON
            return self._export_json(data, channel_name, timestamp)
    
    def _export_xlsx(self, data: Dict[str, Any], channel_name: str, timestamp: str) -> str:
        """Export data to XLSX format."""
        output_file = os.path.join(self.output_dir, f"{channel_name}_{timestamp}.xlsx")
        
        try:
            # Extract posts data
            posts = data.get("posts", [])
            
            if not posts:
                logger.warning("No posts to export")
                return self._export_json(data, channel_name, timestamp)  # Fallback to JSON
                
            # Normalize data for Excel
            flat_posts = []
            for post in posts:
                flat_post = self._flatten_post(post)
                flat_posts.append(flat_post)
                
            # Create DataFrame
            df = pd.DataFrame(flat_posts)
            
            # Create Excel writer
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Write posts to sheet
                df.to_excel(writer, sheet_name='Posts', index=False)
                
                # Write channel info to separate sheet
                channel_info = data.get("channel", {})
                if channel_info:
                    # Convert to DataFrame
                    channel_df = pd.DataFrame([channel_info])
                    channel_df.to_excel(writer, sheet_name='Channel Info', index=False)
                    
                # Write metadata
                metadata = {
                    "Parsed at": data.get("parsed_at", datetime.now().isoformat()),
                    "Total posts": len(posts),
                    "Export timestamp": timestamp
                }
                metadata_df = pd.DataFrame([metadata])
                metadata_df.to_excel(writer, sheet_name='Metadata', index=False)
                
            logger.info(f"Data exported to Excel: {output_file}")
            return output_file
        except Exception as e:
            logger.error(f"Error exporting to Excel: {str(e)}")
            # Fallback to JSON
            return self._export_json(data, channel_name, timestamp)
    
    def _flatten_post(self, post: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten post data for tabular export formats."""
        flat_post = {}
        
        # Copy basic fields
        for key, value in post.items():
            if key == "media" and isinstance(value, list):
                # Join media URLs with semicolons
                flat_post[key] = "; ".join(str(url) for url in value)
            elif isinstance(value, (str, int, float, bool, type(None))):
                flat_post[key] = value
            elif isinstance(value, dict):
                # Flatten nested dictionaries with dot notation
                for nested_key, nested_value in value.items():
                    if isinstance(nested_value, (str, int, float, bool, type(None))):
                        flat_post[f"{key}.{nested_key}"] = nested_value