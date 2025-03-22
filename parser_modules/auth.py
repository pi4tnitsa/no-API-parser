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
from typing import Tuple, Optional, Dict, Any

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
        
    async def login(self, force_login: bool = False) -> Tuple[Browser, Page]:
        """
        Launch browser and log in to Telegram Web if needed.
        
        Args:
            force_login: Force new login even if session exists
            
        Returns:
            Browser and Page instances
        """
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
        await page.goto('https://web.telegram.org/k/', {'waitUntil': 'networkidle0', 'timeout': 100000})
        await asyncio.sleep(5)  # Ждём загрузку

        # Проверяем, залогинен ли пользователь
        if not force_login and await self._is_logged_in(page):
            logger.info("User is already logged in, skipping login process.")
            return browser, page

        # Если сессии нет или требуется повторный вход — выполняем авторизацию
        return await self._perform_login(browser, page)
    
    async def login_as_guest(self) -> Tuple[Browser, Page]:
        """
        Launch browser without logging in (for public channels only).
        
        Returns:
            Browser and Page instances
        """
        browser_args = ['--no-sandbox', '--disable-setuid-sandbox', '--disable-infobars',
                        '--window-size=1366,768', '--disable-dev-shm-usage']

        if self.config.proxy:
            browser_args.append(f'--proxy-server={self.config.proxy}')

        browser = await launch(
            headless=self.config.headless,
            args=browser_args,
            defaultViewport=None
        )

        page = await browser.newPage()
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

        # Go to Telegram Web
        await page.goto('https://web.telegram.org/k/', {'waitUntil': 'networkidle0', 'timeout': 60000})
        logger.info("Started guest session (public channels only)")
        
        return browser, page
    
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
            # Wait for phone input field
            logger.info("Entering phone number")
            country_code_selector = 'div.input-field.input-field-phone > div.input-field-input'
            await page.waitForSelector(country_code_selector, {'visible': True, 'timeout': 10000})
            
            # Clear the input field completely
            # First click on the field
            await page.click(country_code_selector)
            await asyncio.sleep(0.5)
            
            # Use keyboard shortcuts to select all text and delete it
            # First select all text (Ctrl+A or Command+A)
            await page.keyboard.down('Control')  # Or 'Meta' for Mac
            await page.keyboard.press('a')
            await page.keyboard.up('Control')    # Or 'Meta' for Mac
            await asyncio.sleep(0.5)
            
            # Then delete the selected text
            await page.keyboard.press('Backspace')
            await asyncio.sleep(0.5)
            
            # Now type the phone number
            phone_number = self.config.phone.strip()
            await page.type(country_code_selector, phone_number)
            await asyncio.sleep(1)

            # Нажимаем "Next"
            next_button = await page.querySelector("button.btn-primary")
            await next_button.click()
            logger.info("Clicked Next after entering phone number")
            
            # Wait for code input or possible captcha
            await asyncio.sleep(3)
            
            # Check for captcha
            captcha_img = await page.querySelector('img.captcha-image')
            if captcha_img:
                logger.warning("Captcha detected! Please solve it manually")
                # Prompt user to solve captcha
                print("\nCaptcha detected! Please solve it in the browser window.")
                for _ in range(60):  # Wait up to 60 seconds
                    captcha_img = await page.querySelector('img.captcha-image')
                    if not captcha_img:
                        logger.info("Captcha solved")
                        break
                    await asyncio.sleep(1)
                else:
                    logger.error("Captcha solving timed out")
                    raise Exception("Captcha solving timed out")
                
            # Wait for code input
            code_input_selector = 'input.input-field'
            try:
                await page.waitForSelector(code_input_selector, {'visible': True, 'timeout': 60000})
            except Exception as e:
                # Check for 2FA password request
                password_input = await page.querySelector('input[type="password"]')
                if password_input:
                    return await self._handle_2fa(page, browser)
                else:
                    logger.error(f"Failed to find code input: {str(e)}")
                    raise
            
            # Prompt for verification code
            logger.info("Waiting for verification code input")
            print("\nPlease enter the verification code sent to your phone:")
            verification_code = input().strip()
            
            # Enter verification code
            await page.type(code_input_selector, verification_code)
            logger.info("Entered verification code")
            await asyncio.sleep(5)
            
            # Check for 2FA
            password_input = await page.querySelector('input[type="password"]')
            if password_input:
                return await self._handle_2fa(page, browser)
            
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
    
    async def _handle_2fa(self, page: Page, browser: Browser) -> Tuple[Browser, Page]:
        """Handle two-factor authentication."""
        logger.info("Two-factor authentication required")
        print("\nPlease enter your two-factor authentication password:")
        password = input().strip()
        
        # Enter 2FA password
        await page.type('input[type="password"]', password)
        
        # Click submit
        submit_button = await page.querySelector('button.btn-primary')
        if submit_button:
            await submit_button.click()
        
        # Wait for login to complete
        for _ in range(30):  # Wait up to 30 seconds
            if await self._is_logged_in(page):
                logger.info("Successfully logged in with 2FA")
                self._save_session()
                return browser, page
            await asyncio.sleep(1)
            
        logger.error("2FA login failed or timed out")
        raise Exception("2FA login failed or timed out")
    
    async def _is_logged_in(self, page: Page) -> bool:
        """Check if user is already logged in."""
        try:
            # Проверяем, отображается ли список чатов
            chat_list = await page.querySelector('.chat-list, div[data-peer-id]')
            return chat_list is not None
        except Exception:
            return False
    
    async def check_session_validity(self) -> bool:
        """
        Check if the current session is valid.
        
        Returns:
            True if session is valid, False otherwise
        """
        session_file = self._get_session_file()
        if not os.path.exists(session_file):
            return False
            
        try:
            with open(session_file, 'rb') as f:
                session_data = pickle.load(f)
                
            # Check if session is expired (older than 7 days)
            if time.time() - session_data.get('timestamp', 0) > 7 * 24 * 60 * 60:
                logger.warning("Session is expired")
                return False
                
            return True
        except Exception as e:
            logger.error(f"Error checking session validity: {str(e)}")
            return False
    
    async def logout(self) -> None:
        """
        Log out from Telegram Web.
        
        Returns:
            None
        """
        # Start a temporary browser
        browser_args = ['--no-sandbox', '--disable-setuid-sandbox']
        
        if self.config.proxy:
            browser_args.append(f'--proxy-server={self.config.proxy}')
        
        browser = await launch(
            headless=self.config.headless,
            args=browser_args,
            defaultViewport=None,
            userDataDir=self._get_user_data_dir()
        )
        
        try:
            page = await browser.newPage()
            await page.goto('https://web.telegram.org/k/', {'waitUntil': 'networkidle0'})
            
            # Check if logged in
            if not await self._is_logged_in(page):
                logger.info("Already logged out")
                return
                
            # Open settings menu
            menu_button = await page.querySelector('button.btn-menu')
            if menu_button:
                await menu_button.click()
                await asyncio.sleep(1)
                
                # Find and click Settings
                settings_items = await page.querySelectorAll('li.tgico-settings')
                if settings_items:
                    await settings_items[0].click()
                    await asyncio.sleep(1)
                    
                    # Find and click Log Out
                    logout_buttons = await page.querySelectorAll('button.danger')
                    for button in logout_buttons:
                        button_text = await page.evaluate('(el) => el.textContent', button)
                        if 'Log Out' in button_text:
                            await button.click()
                            await asyncio.sleep(1)
                            
                            # Confirm logout
                            confirm_button = await page.querySelector('button.danger')
                            if confirm_button:
                                await confirm_button.click()
                                logger.info("Successfully logged out")
                                
                                # Delete session file
                                if os.path.exists(self._get_session_file()):
                                    os.remove(self._get_session_file())
                                    
                                return
            
            logger.warning("Failed to log out, could not find logout button")
            
        except Exception as e:
            logger.error(f"Error during logout: {str(e)}")
        finally:
            await browser.close()
    
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
