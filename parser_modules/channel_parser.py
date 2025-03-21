"""
Channel parser module
Handles parsing of Telegram channels
"""

import asyncio
import logging
import re
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

from pyppeteer.browser import Browser
from pyppeteer.page import Page

logger = logging.getLogger("telegram_parser.channel_parser")

class ChannelParser:
    """Parser for Telegram channels."""
    
    def __init__(self, browser: Browser, page: Page, config):
        """Initialize with browser, page and config."""
        self.browser = browser
        self.page = page
        self.config = config
        
    async def parse_channel(self, channel_identifier: str) -> Dict[str, Any]:
        """
        Parse a Telegram channel.
        
        Args:
            channel_identifier: Channel username, ID, or URL
            
        Returns:
            Dict containing channel info and posts
        """
        logger.info(f"Parsing channel: {channel_identifier}")
        
        # Navigate to channel
        await self._navigate_to_channel(channel_identifier)
        
        # Get channel info
        channel_info = await self._get_channel_info()
        
        # Get posts
        posts = await self._get_posts()
        
        return {
            "channel": channel_info,
            "posts": posts,
            "parsed_at": datetime.now().isoformat()
        }
        
    async def _navigate_to_channel(self, channel_identifier: str) -> None:
        """Navigate to the channel page."""
        # Clean up the identifier
        if channel_identifier.startswith("@"):
            channel_identifier = channel_identifier[1:]
            
        # Construct URL
        if channel_identifier.startswith("https://"):
            url = channel_identifier
        else:
            url = f"https://web.telegram.org/k/#@{channel_identifier}"
            
        logger.info(f"Navigating to: {url}")
        
        try:
            await self.page.goto(url, {'waitUntil': 'networkidle0', 'timeout': 60000})
            await asyncio.sleep(5)  # Wait for possible redirects and content load
            
            # Check if we're on the channel page
            channel_title = await self._get_channel_title()
            if not channel_title:
                logger.warning(f"Could not find channel: {channel_identifier}")
                raise Exception(f"Channel not found: {channel_identifier}")
                
            logger.info(f"Successfully navigated to channel: {channel_title}")
        except Exception as e:
            logger.error(f"Error navigating to channel {channel_identifier}: {str(e)}")
            raise
            
    async def _get_channel_info(self) -> Dict[str, Any]:
        """Get channel information."""
        channel_info = {}
        
        try:
            # Get channel title
            channel_title = await self._get_channel_title()
            channel_info["title"] = channel_title
            
            # Get channel description
            try:
                description_element = await self.page.querySelector('.chat-info .subtitle')
                if description_element:
                    channel_info["description"] = await self.page.evaluate('(element) => element.textContent', description_element)
            except Exception:
                logger.debug("Could not get channel description")
                
            # Get subscriber count
            try:
                subscribers_element = await self.page.querySelector('.chat-info-container .members')
                if subscribers_element:
                    subscribers_text = await self.page.evaluate('(element) => element.textContent', subscribers_element)
                    subscribers_match = re.search(r'(\d+(?:\.\d+)?[KMG]?)', subscribers_text)
                    if subscribers_match:
                        channel_info["subscribers"] = subscribers_match.group(1)
            except Exception:
                logger.debug("Could not get subscriber count")
                
            # Get channel username/link
            current_url = self.page.url
            if "@" in current_url:
                username = current_url.split("@")[-1]
                channel_info["username"] = username
                channel_info["url"] = f"https://t.me/{username}"
            
            logger.info(f"Channel info: {channel_info}")
            return channel_info
        except Exception as e:
            logger.error(f"Error getting channel info: {str(e)}")
            return {"title": "Unknown", "error": str(e)}
            
    async def _get_channel_title(self) -> Optional[str]:
        """Get the channel title."""
        try:
            title_element = await self.page.querySelector('.chat-info .peer-title')
            if title_element:
                return await self.page.evaluate('(element) => element.textContent', title_element)
            return None
        except Exception:
            return None
            
    async def _get_posts(self) -> List[Dict[str, Any]]:
        """Get posts from the channel."""
        posts = []
        post_limit = self.config.limit
        
        logger.info(f"Getting up to {post_limit} posts")
        
        try:
            # Scroll to load more posts
            last_height = 0
            retries = 0
            max_retries = 5
            
            while len(posts) < post_limit and retries < max_retries:
                # Get all message elements
                message_elements = await self.page.querySelectorAll('message-list-item')
                
                # Process new messages
                for i, msg_element in enumerate(message_elements[len(posts):]):
                    if len(posts) >= post_limit:
                        break
                        
                    try:
                        post_data = await self._extract_post_data(msg_element)
                        if post_data:
                            posts.append(post_data)
                            logger.debug(f"Extracted post {len(posts)}: {post_data.get('id', 'unknown')}")
                    except Exception as e:
                        logger.error(f"Error extracting post data: {str(e)}")
                
                # Scroll down to load more
                current_height = await self.page.evaluate('document.body.scrollHeight')
                if current_height == last_height:
                    retries += 1
                    logger.debug(f"No new content loaded, retry {retries}/{max_retries}")
                else:
                    retries = 0
                    
                last_height = current_height
                await self.page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await asyncio.sleep(2)  # Wait for content to load
                
            logger.info(f"Extracted {len(posts)} posts")
            return posts
            
        except Exception as e:
            logger.error(f"Error getting posts: {str(e)}")
            return posts
    
    async def _extract_post_data(self, message_element) -> Dict[str, Any]:
        """Extract data from a post element."""
        post_data = {}
        
        try:
            # Get post ID
            post_id = await self.page.evaluate('''
                (element) => {
                    const idAttr = element.getAttribute('data-mid');
                    return idAttr || element.id || null;
                }
            ''', message_element)
            post_data["id"] = post_id
            
            # Get post date
            try:
                date_element = await message_element.querySelector('.date')
                if date_element:
                    date_text = await self.page.evaluate('(element) => element.textContent', date_element)
                    post_data["date"] = date_text
                    
                    # Try to get timestamp
                    timestamp = await self.page.evaluate('''
                        (element) => {
                            const timeAttr = element.getAttribute('data-timestamp') || 
                                             element.getAttribute('data-time');
                            return timeAttr ? parseInt(timeAttr, 10) : null;
                        }
                    ''', date_element)
                    
                    if timestamp:
                        post_data["timestamp"] = timestamp
                        post_data["datetime"] = datetime.fromtimestamp(timestamp).isoformat()
            except Exception:
                logger.debug("Could not get post date")
                
            # Get post content
            try:
                content_element = await message_element.querySelector('textContent')
                if content_element:
                    content_text = await self.page.evaluate('''
                        (element) => {
                            const textElements = element.querySelectorAll('.text-content');
                            return Array.from(textElements).map(el => el.textContent).join('\\n');
                        }
                    ''', content_element)
                    post_data["content"] = content_text
            except Exception:
                logger.debug("Could not get post content")
                
            # Get post views
            try:
                views_element = await message_element.querySelector('.views')
                if views_element:
                    views_text = await self.page.evaluate('(element) => element.textContent', views_element)
                    views_match = re.search(r'(\d+(?:\.\d+)?[KMG]?)', views_text)
                    if views_match:
                        post_data["views"] = views_match.group(1)
            except Exception:
                logger.debug("Could not get post views")
                
            # Get post reactions/likes
            try:
                reactions_element = await message_element.querySelector('.reactions')
                if reactions_element:
                    reactions_count = await self.page.evaluate('''
                        (element) => {
                            const counters = element.querySelectorAll('.counter');
                            return Array.from(counters).reduce((sum, el) => {
                                const count = parseInt(el.textContent, 10);
                                return sum + (isNaN(count) ? 0 : count);
                            }, 0);
                        }
                    ''', reactions_element)
                    post_data["reactions"] = reactions_count
            except Exception:
                logger.debug("Could not get post reactions")
                
            # Get post comments
            try:
                comments_element = await message_element.querySelector('.replies')
                if comments_element:
                    comments_text = await self.page.evaluate('(element) => element.textContent', comments_element)
                    comments_match = re.search(r'(\d+(?:\.\d+)?[KMG]?)', comments_text)
                    if comments_match:
                        post_data["comments"] = comments_match.group(1)
            except Exception:
                logger.debug("Could not get post comments")
                
            # Get media attachments
            try:
                media_elements = await message_element.querySelectorAll('.media-container')
                if media_elements and len(media_elements) > 0:
                    media_urls = await self.page.evaluate('''
                        (elements) => {
                            return Array.from(elements).map(el => {
                                const img = el.querySelector('img');
                                const video = el.querySelector('video');
                                if (img && img.src) return img.src;
                                if (video && video.src) return video.src;
                                return null;
                            }).filter(src => src);
                        }
                    ''', media_elements)
                    post_data["media"] = media_urls
            except Exception:
                logger.debug("Could not get post media")
                
            # Get forwarded info
            try:
                forwarded_element = await message_element.querySelector('.forwarded-from')
                if forwarded_element:
                    forwarded_text = await self.page.evaluate('(element) => element.textContent', forwarded_element)
                    post_data["forwarded_from"] = forwarded_text
            except Exception:
                logger.debug("Could not get forwarded info")
                
            return post_data
        except Exception as e:
            logger.error(f"Error extracting post data: {str(e)}")
            return {"id": "unknown", "error": str(e)}