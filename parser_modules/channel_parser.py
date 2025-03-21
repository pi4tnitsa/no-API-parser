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
        
    async def _extract_post_data(self, message_element) -> Dict[str, Any]:
        """Extract data from a post element with improved time extraction."""
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
            
            # Get post date and time - extract only time when available
            try:
                date_element = await message_element.querySelector('.time')
                if not date_element:
                    date_element = await message_element.querySelector('.date')
                
                if date_element:
                    # Extract just the time (HH:MM) from full timestamp
                    time_text = await self.page.evaluate('''
                        (element) => {
                            const fullText = element.textContent.trim();
                            
                            // Try to extract just the time portion (HH:MM)
                            const timeMatch = fullText.match(/(\d{1,2}:\d{2}(?:\s*(?:AM|PM))?)/i);
                            if (timeMatch) {
                                return timeMatch[1];
                            }
                            
                            return fullText; // Fallback to full text if no match
                        }
                    ''', date_element)
                    
                    post_data["time"] = time_text
                    
                    # Also get full datetime for internal use
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
                        post_data["datetime"] = datetime.fromtimestamp(timestamp / 1000).isoformat()
                
            except Exception as e:
                logger.debug(f"Could not get post time: {str(e)}")
                
            # Get post content
            try:
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
                
            # Get post reactions - improved to combine all reactions
            try:
                total_reactions = await self.page.evaluate('''
                    (element) => {
                        // Try finding all reaction counters and sum them
                        const reactionsElements = element.querySelectorAll('.reactions, .reaction-counter, .like-button');
                        let totalCount = 0;
                        
                        for (const reactionElem of reactionsElements) {
                            // For multiple reaction counters, sum them
                            const counters = reactionElem.querySelectorAll('.counter, .reaction-count');
                            if (counters.length > 0) {
                                for (const counter of counters) {
                                    const count = parseInt(counter.textContent.replace(/[^0-9]/g, ''), 10);
                                    if (!isNaN(count)) {
                                        totalCount += count;
                                    }
                                }
                            } else {
                                // For single reaction counter
                                const text = reactionElem.textContent;
                                const match = text.match(/\\d+/);
                                if (match) {
                                    totalCount += parseInt(match[0], 10);
                                }
                            }
                        }
                        
                        return totalCount > 0 ? totalCount : null;
                    }
                ''', message_element)
                
                if total_reactions:
                    post_data["reactions"] = total_reactions
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
                
            # Get media type (without URLs)
            try:
                media_types = await self.page.evaluate('''
                    (element) => {
                        const mediaTypes = [];
                        
                        // Check for photos
                        const photos = element.querySelectorAll('.media-photo, img.photo, .attachment-photo');
                        if (photos.length > 0) {
                            mediaTypes.push('photo');
                        }
                        
                        // Check for videos
                        const videos = element.querySelectorAll('.media-video, video, .attachment-video');
                        if (videos.length > 0) {
                            mediaTypes.push('video');
                        }
                        
                        // Check for documents
                        const docs = element.querySelectorAll('.document, .attachment-document');
                        if (docs.length > 0) {
                            mediaTypes.push('document');
                        }
                        
                        return mediaTypes;
                    }
                ''', message_element)
                
                if media_types and len(media_types) > 0:
                    post_data["media_types"] = media_types
            except Exception as e:
                logger.debug(f"Could not get post media types: {str(e)}")
                
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


    async def _get_posts(self) -> List[Dict[str, Any]]:
        """Get posts from the channel with improved scrolling for lazy loading."""
        posts = []
        post_limit = self.config.limit
        
        logger.info(f"Getting up to {post_limit} posts")
        
        try:
            # Track processed messages to avoid duplicates
            processed_ids = set()
            consecutive_no_new_posts = 0
            max_no_new_attempts = 8  # Increased for more resilient scrolling
            scroll_count = 0
            batch_start_time = time.time()
            
            # Find the correct scrollable container
            scroll_containers = await self.page.evaluate('''
                () => {
                    const containers = [];
                    // Try multiple possible scroll container selectors
                    const selectors = [
                        '.chat-container', 
                        '.bubbles-inner',
                        '.messages-container',
                        '.history-container',
                        '.scrollable'
                    ];
                    
                    for (const selector of selectors) {
                        const element = document.querySelector(selector);
                        if (element) {
                            containers.push({
                                selector,
                                scrollHeight: element.scrollHeight,
                                clientHeight: element.clientHeight,
                                scrollTop: element.scrollTop
                            });
                        }
                    }
                    
                    return containers;
                }
            ''')
            
            if not scroll_containers:
                logger.error("Could not find scrollable container!")
                return posts
                
            logger.info(f"Found scroll containers: {scroll_containers}")
            
            # Choose the container with the largest scrollHeight (likely the main message container)
            main_container = max(scroll_containers, key=lambda x: x['scrollHeight'])
            container_selector = main_container['selector']
            logger.info(f"Using scrollable container: {container_selector}")
            
            # Initialize scrolling position to start at the bottom (newest messages)
            await self.page.evaluate(f'''
                () => {{
                    const container = document.querySelector('{container_selector}');
                    if (container) {{
                        // Scroll to the very bottom
                        container.scrollTop = container.scrollHeight;
                        console.log('Started at newest messages, scroll position:', container.scrollTop);
                    }}
                }}
            ''')
            
            # Wait for initial messages to load
            await asyncio.sleep(4)
            
            while len(posts) < post_limit and consecutive_no_new_posts < max_no_new_attempts:
                # Process currently visible messages
                message_elements = await self.page.querySelectorAll('.message, .bubble, .message-list-item')
                
                if not message_elements:
                    logger.warning("No message elements found with any selector!")
                    consecutive_no_new_posts += 1
                    
                    # Try more aggressive scrolling if no messages found
                    await self.page.evaluate(f'''
                        () => {{
                            const container = document.querySelector('{container_selector}');
                            if (container) {{
                                container.scrollTop -= 1500;  // Larger scroll
                            }}
                        }}
                    ''')
                    await asyncio.sleep(2)
                    continue
                
                logger.debug(f"Processing {len(message_elements)} visible messages")
                new_posts_in_batch = 0
                
                # Process visible messages
                for msg_element in message_elements:
                    if len(posts) >= post_limit:
                        break
                        
                    try:
                        # Get message ID
                        msg_id = await self.page.evaluate('''
                            (element) => {
                                return element.getAttribute('data-mid') || 
                                    element.getAttribute('data-message-id') ||
                                    element.id || 
                                    element.getAttribute('data-message') ||
                                    null;
                            }
                        ''', msg_element)
                        
                        # Skip if no ID or already processed
                        if not msg_id or msg_id in processed_ids:
                            continue
                        
                        processed_ids.add(msg_id)
                        
                        # Check date range if needed
                        if not await self._is_date_in_range(msg_element):
                            if self.config.start_date:
                                # Check if we've gone past the start date
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
                                        logger.info(f"Found messages older than start date, stopping")
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
                
                # If we have enough posts, stop scrolling
                if len(posts) >= post_limit:
                    break
                    
                # Check scroll position
                scroll_info = await self.page.evaluate(f'''
                    () => {{
                        const container = document.querySelector('{container_selector}');
                        if (container) {{
                            return {{
                                scrollTop: container.scrollTop,
                                scrollHeight: container.scrollHeight,
                                clientHeight: container.clientHeight,
                                atTop: container.scrollTop <= 10
                            }};
                        }}
                        return null;
                    }}
                ''')
                
                if scroll_info:
                    logger.debug(f"Scroll position: {scroll_info['scrollTop']} / {scroll_info['scrollHeight']}")
                    
                    # Check if we've reached the top
                    if scroll_info['atTop']:
                        logger.info("Reached the top of the chat, no more messages")
                        break
                
                # Use a more aggressive scrolling approach to ensure more messages load
                scroll_changed = await self.page.evaluate(f'''
                    () => {{
                        const container = document.querySelector('{container_selector}');
                        if (container) {{
                            const oldScrollTop = container.scrollTop;
                            
                            // Scroll up by a significant amount
                            container.scrollTop -= 1200;
                            
                            // Force a redraw to ensure DOM updates
                            container.style.display = 'none';
                            void container.offsetHeight; // Trigger reflow
                            container.style.display = '';
                            
                            return {{
                                changed: oldScrollTop !== container.scrollTop,
                                oldPos: oldScrollTop,
                                newPos: container.scrollTop
                            }};
                        }}
                        return {{ changed: false }};
                    }}
                ''')
                
                scroll_count += 1
                
                # Log scroll result
                if scroll_changed:
                    logger.debug(f"Scroll result: {scroll_changed}")
                
                # Give DOM time to update with new messages
                await asyncio.sleep(2)
                
                # Log batch performance
                batch_end_time = time.time()
                batch_duration = batch_end_time - batch_start_time
                logger.info(f"Batch processed: {new_posts_in_batch} new posts in {batch_duration:.2f}s (total: {len(posts)}/{post_limit})")
                batch_start_time = batch_end_time
                
                # Monitor progress
                if new_posts_in_batch == 0:
                    consecutive_no_new_posts += 1
                    logger.debug(f"No new posts in batch, attempt {consecutive_no_new_posts}/{max_no_new_attempts}")
                    
                    if consecutive_no_new_posts >= 3:
                        # After a few failed attempts, try a more extreme scroll and wait longer
                        await self.page.evaluate(f'''
                            () => {{
                                const container = document.querySelector('{container_selector}');
                                if (container) {{
                                    // More extreme scroll to try to trigger loading
                                    container.scrollTop = Math.max(0, container.scrollTop - 2000);
                                }}
                            }}
                        ''')
                        await asyncio.sleep(3)  # Longer wait for content to load
                else:
                    consecutive_no_new_posts = 0
            
            logger.info(f"Extracted {len(posts)} posts with {scroll_count} scroll operations")
            return posts
            
        except Exception as e:
            logger.error(f"Error getting posts: {str(e)}")
            return posts
