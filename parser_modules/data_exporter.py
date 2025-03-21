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
from typing import Dict, Any, List, Optional, Tuple, Union

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
    
    def export_multiple_channels(self, data_list: List[Dict[str, Any]], output_prefix: str = "multi_channel") -> Tuple[str, List[str]]:
        """
        Export data from multiple channels to a single file or multiple files.
        
        Args:
            data_list: List of channel data dictionaries
            output_prefix: Prefix for the output filename
            
        Returns:
            Tuple of (combined file path, list of individual file paths)
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_format = self.config.export_format.lower()
        
        # Export individual channels
        file_paths = []
        for data in data_list:
            channel_name = data.get("channel", {}).get("name", "unknown")
            channel_name = self._sanitize_filename(channel_name)
            file_path = self.export_data(data, channel_name)
            file_paths.append(file_path)
            
        # Export combined data if possible
        combined_file = ""
        
        # For XLSX, we can create a workbook with multiple sheets
        if export_format == "xlsx":
            combined_file = os.path.join(self.output_dir, f"{output_prefix}_{timestamp}.xlsx")
            try:
                with pd.ExcelWriter(combined_file, engine='openpyxl') as writer:
                    # Add each channel to its own sheet
                    for data in data_list:
                        channel_info = data.get("channel", {})
                        channel_name = channel_info.get("name", "unknown")
                        safe_channel_name = self._sanitize_filename(channel_name)
                        
                        # Limit sheet name to 31 characters (Excel limitation)
                        sheet_name = safe_channel_name[:31]
                        
                        # Export posts
                        posts = data.get("posts", [])
                        if posts:
                            flat_posts = [self._flatten_post(post) for post in posts]
                            df = pd.DataFrame(flat_posts)
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                            
                    # Create a summary sheet
                    summary_data = []
                    for data in data_list:
                        channel_info = data.get("channel", {})
                        summary_data.append({
                            "Channel Name": channel_info.get("name", "unknown"),
                            "Username": channel_info.get("username", ""),
                            "Posts Count": len(data.get("posts", [])),
                            "Parsed At": data.get("parsed_at", "")
                        })
                    
                    if summary_data:
                        summary_df = pd.DataFrame(summary_data)
                        summary_df.to_excel(writer, sheet_name="Summary", index=False)
                
                logger.info(f"Combined data exported to Excel: {combined_file}")
            except Exception as e:
                logger.error(f"Error exporting combined data to Excel: {str(e)}")
                combined_file = ""
                
        return combined_file, file_paths
    
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
            try:
                with open(fallback_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                logger.warning(f"Used fallback filename for JSON export: {fallback_file}")
                return fallback_file
            except Exception as fallback_error:
                logger.error(f"Critical export error: {str(fallback_error)}")
                raise RuntimeError(f"Failed to export data: {str(e)}, fallback also failed: {str(fallback_error)}")
    
    def _export_csv(self, data: Dict[str, Any], channel_name: str, timestamp: str) -> str:
        """Export data to CSV format."""
        output_file = os.path.join(self.output_dir, f"{channel_name}_{timestamp}.csv")
        
        try:
            # Extract posts data
            posts = data.get("posts", [])
            
            if not posts:
                logger.warning("No posts to export to CSV, falling back to JSON")
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
            logger.warning("Falling back to JSON export")
            # Fallback to JSON
            return self._export_json(data, channel_name, timestamp)
    
    def _export_xlsx(self, data: Dict[str, Any], channel_name: str, timestamp: str) -> str:
        """Export data to XLSX format."""
        output_file = os.path.join(self.output_dir, f"{channel_name}_{timestamp}.xlsx")
        
        try:
            # Extract posts data
            posts = data.get("posts", [])
            
            if not posts:
                logger.warning("No posts to export to Excel, falling back to JSON")
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
            logger.warning("Falling back to JSON export")
            # Fallback to JSON
            return self._export_json(data, channel_name, timestamp)
    
    def _flatten_post(self, post: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        """
        Flatten post data recursively for tabular export formats.
        
        Args:
            post: The post data to flatten
            prefix: Prefix for nested keys
            
        Returns:
            Flattened dictionary
        """
        flat_post = {}
        
        for key, value in post.items():
            # Create the full key with prefix
            full_key = f"{prefix}.{key}" if prefix else key
            
            # Handle special cases
            if key == "media" and isinstance(value, list):
                # Join media URLs with semicolons
                flat_post[full_key] = "; ".join(str(url) for url in value if url)
            # Handle different data types
            elif isinstance(value, (str, int, float, bool, type(None))):
                flat_post[full_key] = value
            elif isinstance(value, dict):
                # Recursively flatten nested dictionaries
                nested_flat = self._flatten_post(value, full_key)
                flat_post.update(nested_flat)
            elif isinstance(value, list):
                # Handle lists - try to convert to string
                try:
                    items = [str(item) for item in value if item is not None]
                    flat_post[full_key] = "; ".join(items)
                except Exception:
                    flat_post[full_key] = f"[List with {len(value)} items]"
            else:
                # Handle other types by converting to string
                try:
                    flat_post[full_key] = str(value)
                except Exception:
                    flat_post[full_key] = f"[Unsupported type: {type(value).__name__}]"
        
        return flat_post
