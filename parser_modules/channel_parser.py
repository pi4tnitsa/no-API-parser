"""
Channel parser module
Handles parsing of Telegram channels
"""

import asyncio
import logging
import re
import time
from datetime import datetime, timedelta
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
                # Updated selector for channel description
                description_element = await self.page.querySelector('.chat-info .info .subtitle')
                if description_element:
                    channel_info["description"] = await self.page.evaluate('(element) => element.textContent', description_element)
            except Exception as e:
                logger.debug(f"Could not get channel description: {str(e)}")
                
            # Get subscriber count
            try:
                # Updated selector for subscribers count
                subscribers_element = await self.page.querySelector('.chat-info-container .profile-subtitle')
                if subscribers_element:
                    subscribers_text = await self.page.evaluate('(element) => element.textContent', subscribers_element)
                    subscribers_match = re.search(r'(\d+(?:\.\d+)?[KMG]?)', subscribers_text)
                    if subscribers_match:
                        channel_info["subscribers"] = subscribers_match.group(1)
            except Exception as e:
                logger.debug(f"Could not get subscriber count: {str(e)}")
                
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
            # Updated selector for channel title
            title_element = await self.page.querySelector('.chat-info .peer-title')
            if title_element:
                return await self.page.evaluate('(element) => element.textContent', title_element)
            return None
        except Exception:
            return None

    async def _is_date_in_range(self, element) -> bool:
        """Check if message date is within the specified date range."""
        if not (self.config.start_date or self.config.end_date):
            return True  # No date filtering
            
        try:
            # Get date element
            date_element = await element.querySelector('.message-date-group')
            if not date_element:
                date_element = await element.querySelector('.time')
                
            if not date_element:
                return True  # Can't determine date, include by default
                
            date_text = await self.page.evaluate('(element) => element.textContent', date_element)
            
            # Try to parse the date
            # First check if it has a timestamp attribute
            timestamp = await self.page.evaluate('''
                (element) => {
                    const timeElem = element.querySelector('[data-timestamp]');
                    if (timeElem) {
                        return parseInt(timeElem.getAttribute('data-timestamp'), 10);
                    }
                    return null;
                }
            ''', element)
            
            if timestamp:
                msg_date = datetime.fromtimestamp(timestamp / 1000)  # Convert from ms to seconds
            else:
                # Try to parse from text
                # Handle different date formats (today, yesterday, date)
                current_date = datetime.now()
                
                if "today" in date_text.lower():
                    msg_date = current_date
                elif "yesterday" in date_text.lower():
                    msg_date = current_date - timedelta(days=1)
                else:
                    # Try to parse the date
                    try:
                        # Attempt multiple date formats
                        date_formats = [
                            "%d.%m.%Y",  # 01.01.2023
                            "%d.%m.%y",   # 01.01.23
                            "%b %d",      # Jan 01
                            "%d %b",      # 01 Jan
                            "%B %d",      # January 01
                            "%d %B"       # 01 January
                        ]
                        
                        for fmt in date_formats:
                            try:
                                # Add year if not in format
                                if "%y" not in fmt and "%Y" not in fmt:
                                    parsed_date = datetime.strptime(date_text, fmt)
                                    # Set the year to current year
                                    msg_date = parsed_date.replace(year=current_date.year)
                                    # If the date is in the future, it's probably from last year
                                    if msg_date > current_date:
                                        msg_date = msg_date.replace(year=current_date.year - 1)
                                    break
                                else:
                                    msg_date = datetime.strptime(date_text, fmt)
                                    break
                            except ValueError:
                                continue
                        else:
                            # If no format matched, default to include the message
                            return True
                    except Exception:
                        # If parsing fails, include the message
                        return True
            
            # Check against date range
            if self.config.start_date and msg_date.date() < self.config.start_date:
                return False
                
            if self.config.end_date and msg_date.date() > self.config.end_date:
                return False
                
            return True
            
        except Exception as e:
            logger.debug(f"Error checking date range: {str(e)}")
            return True  # Include by default if there's an error
            
    async def _get_posts(self) -> List[Dict[str, Any]]:
        """Get posts from the channel in chronological order from newest to oldest."""
        posts = []
        post_limit = self.config.limit
        
        logger.info(f"Getting up to {post_limit} posts from newest to oldest")
        
        try:
            # Track processed messages to avoid duplicates
            processed_ids = set()
            consecutive_no_new_posts = 0
            max_no_new_attempts = 5
            scroll_batch_size = 10  # Number of scrolls before processing messages
            scroll_count = 0
            batch_start_time = time.time()
            
            # Initialize scrolling position to start at the bottom (newest messages)
            await self.page.evaluate('''
                () => {
                    const middleColumn = document.querySelector('#MiddleColumn');
                    if (middleColumn) {
                        // Scroll to the very bottom to ensure we start with newest messages
                        middleColumn.scrollTop = middleColumn.scrollHeight;
                        console.log('Initialized to newest messages at:', middleColumn.scrollTop);
                    }
                }
            ''')
            
            # Wait for initial messages to load properly
            await asyncio.sleep(3)  
            
            while len(posts) < post_limit and consecutive_no_new_posts < max_no_new_attempts:
                # Get all visible message elements (starting with newest)
                message_elements = await self.page.querySelectorAll('.message, .bubble, .message-list-item')
                
                if not message_elements:
                    logger.debug("No message elements found with current selectors")
                    consecutive_no_new_posts += 1
                    continue
                
                logger.debug(f"Found {len(message_elements)} message elements visible")
                new_posts_in_batch = 0
                
                # Process visible messages (newest first)
                for msg_element in message_elements:
                    if len(posts) >= post_limit:
                        break
                        
                    try:
                        # Get message ID to avoid duplicates
                        msg_id = await self.page.evaluate('''
                            (element) => {
                                return element.getAttribute('data-mid') || 
                                    element.getAttribute('data-message-id') ||
                                    element.id || 
                                    null;
                            }
                        ''', msg_element)
                        
                        if not msg_id or msg_id in processed_ids:
                            continue
                        
                        processed_ids.add(msg_id)
                        
                        # Check date range
                        in_range = await self._is_date_in_range(msg_element)
                        
                        # If we're checking dates and this message is too old
                        if not in_range and self.config.start_date:
                            date_element = await msg_element.querySelector('.time, .date')
                            if date_element:
                                date_text = await self.page.evaluate('(element) => element.textContent', date_element)
                                logger.debug(f"Message with date {date_text} outside date range")
                                
                                # Get timestamp if available
                                timestamp = await self.page.evaluate('''
                                    (element) => {
                                        const timeElem = element.querySelector('[data-timestamp]');
                                        if (timeElem) {
                                            return parseInt(timeElem.getAttribute('data-timestamp'), 10);
                                        }
                                        return null;
                                    }
                                ''', msg_element)
                                
                                if timestamp:
                                    msg_date = datetime.fromtimestamp(timestamp / 1000)
                                    if msg_date.date() < self.config.start_date:
                                        logger.info(f"Found messages older than start date, stopping scroll")
                                        consecutive_no_new_posts = max_no_new_attempts  # Force stop
                            continue
                        
                        # Extract post data
                        post_data = await self._extract_post_data(msg_element)
                        
                        if post_data and "id" in post_data:
                            posts.append(post_data)
                            new_posts_in_batch += 1
                            logger.debug(f"Extracted post {len(posts)}/{post_limit}: {post_data.get('id', 'unknown')}")
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
                
                # If we have enough posts or found too old messages, stop scrolling
                if len(posts) >= post_limit or consecutive_no_new_posts >= max_no_new_attempts:
                    break
                    
                # Check if we're at the top of the chat
                at_top = await self.page.evaluate('''
                    () => {
                        const middleColumn = document.querySelector('#MiddleColumn');
                        return middleColumn && middleColumn.scrollTop <= 10;
                    }
                ''')
                
                if at_top:
                    logger.info("Reached the top of the chat history, no more messages")
                    break
                    
                # Scroll up to load older messages
                scroll_changed = False
                for _ in range(scroll_batch_size):
                    scroll_result = await self.page.evaluate('''
                        () => {
                            const middleColumn = document.querySelector('#MiddleColumn');
                            if (middleColumn) {
                                // Check if we're already at the top
                                if (middleColumn.scrollTop <= 10) {
                                    return false;
                                }
                                
                                // Store previous position
                                const oldScrollTop = middleColumn.scrollTop;
                                
                                // Scroll up by 800px to load older messages
                                middleColumn.scrollTop -= 800;
                                
                                // Return true if the scroll position changed
                                return oldScrollTop !== middleColumn.scrollTop;
                            }
                            return false;
                        }
                    ''')
                    
                    if scroll_result:
                        scroll_changed = True
                        scroll_count += 1
                        
                    # Small pause between scrolls
                    await asyncio.sleep(0.3)  
                
                # Log scroll status
                scroll_status = await self.page.evaluate('''
                    () => {
                        const middleColumn = document.querySelector('#MiddleColumn');
                        if (middleColumn) {
                            return {
                                scrollTop: middleColumn.scrollTop,
                                scrollHeight: middleColumn.scrollHeight,
                                clientHeight: middleColumn.clientHeight
                            };
                        }
                        return null;
                    }
                ''')
                
                if scroll_status:
                    logger.debug(f"Scroll position: {scroll_status['scrollTop']} / {scroll_status['scrollHeight']}")
                
                # Give time for content to load after scrolling
                await asyncio.sleep(1.5)
                
                # Log batch performance
                batch_end_time = time.time()
                batch_duration = batch_end_time - batch_start_time
                logger.info(f"Batch processed: {new_posts_in_batch} new posts in {batch_duration:.2f}s (total: {len(posts)}/{post_limit})")
                batch_start_time = batch_end_time
                
                # Check if we found new posts or scrolling was effective
                if new_posts_in_batch == 0 or not scroll_changed:
                    consecutive_no_new_posts += 1
                    logger.debug(f"No new content, attempt {consecutive_no_new_posts}/{max_no_new_attempts}")
                else:
                    consecutive_no_new_posts = 0
            
            logger.info(f"Extracted {len(posts)} posts with {scroll_count} scroll operations")
            
            # No need to sort - posts are already in order from newest to oldest
            # because we started at the bottom and scrolled up
            
            return posts
            
        except Exception as e:
            logger.error(f"Error getting posts: {str(e)}")
            return posts
    
    async def _extract_post_data(self, message_element) -> Dict[str, Any]:
        """Extract data from a post element with updated selectors."""
        post_data = {}
        
        try:
            # Get post ID
            post_id = await self.page.evaluate('''
                (element) => {
                    return element.getAttribute('data-mid') || 
                           element.getAttribute('data-message-id') || 
                           element.id || 
                           null;
                }
            ''', message_element)
            post_data["id"] = post_id
            
            # Get post date
            try:
                # Try multiple date selectors
                date_element = await message_element.querySelector('.time')
                if not date_element:
                    date_element = await message_element.querySelector('.date')
                
                if date_element:
                    date_text = await self.page.evaluate('(element) => element.textContent', date_element)
                    post_data["date"] = date_text
                    
                    # Try to get timestamp
                    timestamp = await self.page.evaluate('''
                        (element) => {
                            if (element.hasAttribute('data-timestamp')) {
                                return parseInt(element.getAttribute('data-timestamp'), 10);
                            }
                            return null;
                        }
                    ''', date_element)
                    
                    if timestamp:
                        post_data["timestamp"] = timestamp
                        post_data["datetime"] = datetime.fromtimestamp(timestamp / 1000).isoformat()  # Convert ms to seconds
            except Exception as e:
                logger.debug(f"Could not get post date: {str(e)}")
                
            # Get post content
            try:
                # Try multiple content selectors
                content_element = await message_element.querySelector('.message-content')
                if not content_element:
                    content_element = await message_element.querySelector('.text-content')
                if not content_element:
                    content_element = await message_element.querySelector('.bubble-content')
                
                if content_element:
                    content_text = await self.page.evaluate('''
                        (element) => {
                            const textElements = element.querySelectorAll('.text-content, .message-text');
                            if (textElements.length > 0) {
                                return Array.from(textElements).map(el => el.textContent).join('\\n');
                            }
                            return element.textContent;
                        }
                    ''', content_element)
                    post_data["content"] = content_text
            except Exception as e:
                logger.debug(f"Could not get post content: {str(e)}")
                
            # Get post views
            try:
                views_element = await message_element.querySelector('.views, .message-views')
                if views_element:
                    views_text = await self.page.evaluate('(element) => element.textContent', views_element)
                    views_match = re.search(r'(\d+(?:\.\d+)?[KMG]?)', views_text)
                    if views_match:
                        post_data["views"] = views_match.group(1)
            except Exception as e:
                logger.debug(f"Could not get post views: {str(e)}")
                
            # Get post reactions/likes
            try:
                reactions_count = await self.page.evaluate('''
                    (element) => {
                        // Try multiple selectors for reactions
                        const reactionsElement = element.querySelector('.reactions, .reaction-counter, .like-button');
                        if (!reactionsElement) return null;
                        
                        // For new Telegram Web interface
                        const counters = reactionsElement.querySelectorAll('.counter, .reaction-count');
                        if (counters.length > 0) {
                            return Array.from(counters).reduce((sum, el) => {
                                const count = parseInt(el.textContent, 10);
                                return sum + (isNaN(count) ? 0 : count);
                            }, 0);
                        }
                        
                        // Fallback to using textContent
                        const text = reactionsElement.textContent;
                        const match = text.match(/\\d+/);
                        return match ? parseInt(match[0], 10) : 0;
                    }
                ''', message_element)
                
                if reactions_count:
                    post_data["reactions"] = reactions_count
            except Exception as e:
                logger.debug(f"Could not get post reactions: {str(e)}")
                
            # Get post comments
            try:
                comments_element = await message_element.querySelector('.replies, .comments-button, .comments-count')
                if comments_element:
                    comments_text = await self.page.evaluate('(element) => element.textContent', comments_element)
                    comments_match = re.search(r'(\d+(?:\.\d+)?[KMG]?)', comments_text)
                    if comments_match:
                        post_data["comments"] = comments_match.group(1)
            except Exception as e:
                logger.debug(f"Could not get post comments: {str(e)}")
                
            # Get media attachments
            try:
                # Updated selectors for media
                media_data = await self.page.evaluate('''
                    (element) => {
                        const media = [];
                        
                        // Photos
                        const photos = element.querySelectorAll('.media-photo, img.photo, .attachment-photo');
                        photos.forEach(photo => {
                            const src = photo.src || photo.dataset.src;
                            if (src) media.push({type: 'photo', url: src});
                        });
                        
                        // Videos
                        const videos = element.querySelectorAll('.media-video, video, .attachment-video');
                        videos.forEach(video => {
                            const src = video.src || video.dataset.src;
                            if (src) media.push({type: 'video', url: src});
                        });
                        
                        // Documents
                        const docs = element.querySelectorAll('.document, .attachment-document');
                        docs.forEach(doc => {
                            const nameElem = doc.querySelector('.document-name, .filename');
                            const name = nameElem ? nameElem.textContent : 'Document';
                            media.push({type: 'document', name: name});
                        });
                        
                        return media;
                    }
                ''', message_element)
                
                if media_data and len(media_data) > 0:
                    post_data["media"] = media_data
            except Exception as e:
                logger.debug(f"Could not get post media: {str(e)}")
                
            # Get forwarded info
            try:
                forwarded_element = await message_element.querySelector('.forwarded-from, .forward-name')
                if forwarded_element:
                    forwarded_text = await self.page.evaluate('(element) => element.textContent', forwarded_element)
                    post_data["forwarded_from"] = forwarded_text
            except Exception as e:
                logger.debug(f"Could not get forwarded info: {str(e)}")
                
            return post_data
        except Exception as e:
            logger.error(f"Error extracting post data: {str(e)}")
            return {"id": "unknown", "error": str(e)}
