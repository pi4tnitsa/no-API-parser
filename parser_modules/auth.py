"""
Telegram authentication module
Handles logging in to Telegram Web and maintaining the session
"""

import asyncio
import logging
import os
import pickle
import random
import time
from typing import Tuple, Optional

from pyppeteer import launch
from pyppeteer.browser import Browser
from pyppeteer.page import Page

logger = logging.getLogger("telegram_parser.auth")

class TelegramAuth:
    """Handles authentication with Telegram Web."""
    
    def __init__(self, config):
        """Initialize the auth module with config."""
        self.config = config
        self.session_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sessions")
        os.makedirs(self.session_dir, exist_ok=True)
        
    async def login(self) -> Tuple[Browser, Page]:
        """Launch browser and log in to Telegram Web if needed."""
        browser_args = ['--no-sandbox', '--disable-setuid-sandbox', '--disable-infobars',
                        '--window-size=1366,768', '--disable-dev-shm-usage']

        if self.config.proxy:
            browser_args.append(f'--proxy-server={self.config.proxy}')

        browser = await launch(
            headless=self.config.headless,
            args=browser_args,
            defaultViewport=None,
            userDataDir=self._get_user_data_dir()
        )

        page = await browser.newPage()
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

        # Переходим на Telegram Web
        await page.goto('https://web.telegram.org/k/', {'waitUntil': 'networkidle0', 'timeout': 60000})
        await asyncio.sleep(5)  # Ждём загрузку

        # Проверяем, залогинен ли пользователь
        if await self._is_logged_in(page):
            logger.info("User is already logged in, skipping login process.")
            return browser, page

        # Если сессии нет — выполняем авторизацию
        return await self._perform_login(browser, page)

    
    async def _perform_login(self, browser: Browser, page: Page) -> Tuple[Browser, Page]:
        """Perform the login process."""
        logger.info("Starting login process")
        
        await page.goto('https://web.telegram.org/k/', {'waitUntil': 'networkidle0', 'timeout': 60000})
        await asyncio.sleep(3)
        
        # Check if we need to log in
        if await self._is_logged_in(page):
            logger.info("Already logged in")
            return browser, page
        
        # Click on "Log in by phone Number"
        try:
            logger.info("Clicking on 'Log in by phone Number'")
            await page.waitForSelector('button.btn-primary', {'timeout': 10000})
            await page.click('button.btn-primary')
        except Exception as e:
            logger.info(f"Phone number button not found, may already be at phone input: {str(e)}")
        
        # Enter phone number
        try:
            # Enter phone number
            # Меняем код страны
            logger.info("Changing country code")
            country_code_selector = 'div.input-field.input-field-phone > div.input-field-input'
            await page.waitForSelector(country_code_selector, {'visible': True, 'timeout': 10000})
            country_code_field = await page.querySelector(country_code_selector)

            # Очищаем поле и вводим код страны
            await page.evaluate('(el) => el.innerText = ""', country_code_field)
            await country_code_field.click()
            await asyncio.sleep(1)

            # Меняем код страны
            logger.info("Changing country code")
            country_code_selector = 'div.input-field.input-field-phone > div.input-field-input'
            await page.waitForSelector(country_code_selector, {'visible': True, 'timeout': 10000})
            country_code_field = await page.querySelector(country_code_selector)

            # Очищаем поле и вводим код страны
            await page.evaluate('(el) => el.innerText = ""', country_code_field)
            await country_code_field.click()
            await asyncio.sleep(1)

            new_country_code = self.config.phone   # Замени на нужный код
            await country_code_field.type(new_country_code)
            await asyncio.sleep(1)

            # Нажимаем "Next"
            next_button = await page.querySelector("button.btn-primary")
            await next_button.click()
            logger.info("Clicked Next after entering country code")
            
            # Wait for code input or possible captcha
            await asyncio.sleep(3)
            
            # Check for captcha
            captcha_img = await page.querySelector('img.captcha-image')
            if captcha_img:
                logger.warning("Captcha detected! Please solve it manually")
                await asyncio.sleep(30)  # Give time to solve manually
                
            # Wait for code input
            await page.waitForSelector('input.input-field', {'visible': True, 'timeout': 60000})
            
            # Prompt for verification code
            logger.info("Waiting for verification code input")
            print("\nPlease enter the verification code sent to your phone:")
            verification_code = input()
            
            # Enter verification code
            await page.type('input.input-field', verification_code)
            logger.info("Entered verification code")
            await asyncio.sleep(10)
            
            # Wait for login to complete
            for _ in range(30):  # Wait up to 30 seconds
                if await self._is_logged_in(page):
                    logger.info("Successfully logged in")
                    self._save_session()
                    return browser, page
                await asyncio.sleep(1)
                
            logger.error("Login failed or timed out")
            raise Exception("Login failed or timed out")
            
        except Exception as e:
            logger.error(f"Login error: {str(e)}")
            raise
    
    async def _is_logged_in(self, page: Page) -> bool:
        """Check if user is already logged in."""
        try:
            # Проверяем, отображается ли список чатов
            chat_list = await page.querySelector('.chat-list, div[data-peer-id]')
            return chat_list is not None
        except Exception:
            return False

    
    def _get_user_data_dir(self) -> str:
        """Get path to user data directory for persistent browser session."""
        return os.path.join(self.session_dir, f"user_data_{self.config.phone}")
    
    def _get_session_file(self) -> str:
        """Get path to session file."""
        return os.path.join(self.session_dir, f"session_{self.config.phone}.pickle")
    
    def _save_session(self) -> None:
        """Save current session data."""
        try:
            session_data = {
                'timestamp': time.time(),
                'phone': self.config.phone
            }
            with open(self._get_session_file(), 'wb') as f:
                pickle.dump(session_data, f)
            logger.info("Session saved successfully")
        except Exception as e:
            logger.error(f"Error saving session: {str(e)}")