"""lass."""

import logging
import sqlite3
from typing import ClassVar

import boto3
import requests
from azure.storage.blob import BlobServiceClient
from google.cloud import storage
from playwright.async_api import TimeoutError, async_playwright

from core import make_path

logger = logging.getLogger(__name__)


class SQL:
    """A class for handling SQLite database operations.

    Methods:
        create_table()
        query()
        get()
        close()

    Attributes:
        fdb (str): Filepath to the SQLite database file.
        con (sqlite3.Connection): Connection to the SQLite database.
        cur (sqlite3.Cursor): Cursor object for executing SQL commands.
    """

    def __init__(self, file_db):
        """Initialize the SQL object with a database filepath.

        Args:
            file_db (str): Filepath to the SQLite database file.
        """
        self.fdb = file_db
        self.con = self._create_connection()
        self.cur = self._create_cursor()

    def _create_connection(self):
        """Establish and return a connection to the SQLite database."""
        return sqlite3.connect(
            self.fdb,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            check_same_thread=False,
        )

    def _create_cursor(self):
        """Create and return a cursor object for SQL operations."""
        return self.con.cursor()

    def create_table(self, table, columns_list):
        """Create a table with the given name and columns.

        Args:
            table (str): Name of the table to create.
            columns_list (list): List of columns with their data types.
        """
        columns = ", ".join(columns_list)
        self.query(f"CREATE TABLE IF NOT EXISTS {table} ({columns})")

    def query(self, sql):
        """Execute a SQL query on the database.

        Args:
            sql (str): The SQL query to execute.

        Returns:
            None
        """
        self.cur.execute(sql)
        self.con.commit()

    def get(self, sql):
        """Execute a SQL query and return the fetched results.

        Args:
            sql (str): The SQL query to execute.

        Returns:
            list: List of rows as tuples.
        """
        self.cur.execute(sql)
        return self.cur.fetchall()

    def close(self):
        """Commit any pending transactions and close the database."""
        if self.con:
            self.con.commit()
            self.cur.close()
            self.con.close()


class Requests:
    """A class for managing HTTP requests with optional proxy support.

    Methods:
        update_header()
        get()

    Attributes:
        HEADER_DEFAULT (ClassVar[dict]): Default HTTP headers for all instances.
        urlp (str): The URL of the proxy service.
        keyp (str): API key for the proxy service.
        sess (requests.Session): Session object for making HTTP requests.
    """

    HEADER_DEFAULT: ClassVar[dict] = {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/128.0",  # noqa: E501
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }

    def __init__(self, proxy_url=None, proxy_key=None):
        """Initialize a Requests instance with optional proxy details.

        Args:
            proxy_url (str, optional): The URL of the proxy service.
            proxy_key (str, optional): API key for the proxy service.
        """
        self.purl = proxy_url
        self.pkey = proxy_key
        self.sess = requests.Session()
        self.sess.headers.update(self.HEADER_DEFAULT)

    def update_header(self, header_custom):
        """Update the HTTP headers for the current session.

        Args:
            header_custom (dict): Custom headers to update the session with.

        Returns:
            None
        """
        self.HEADER_DEFAULT.update(header_custom)
        self.sess.headers.update(self.HEADER_DEFAULT)

    def get(self, url, timeout=10, **kwargs):
        """Perform a GET request on the given URL with optional proxy support.

        Args:
            url (str): The URL to make the request to.
            timeout (int, optional): Time before the request times out.
            **kwargs: Additional parameters to pass in the GET request.

        Returns:
            requests.Response: The response object for the GET request.
        """
        params = None
        if self.purl and self.pkey:
            url, params = (self.purl, {"api_key": self.pkey, "url": url})
        return self.sess.get(
            url=url,
            timeout=timeout,
            params={**params, **kwargs} if params else kwargs,
        )


class Browser:
    """A class for browser interaction using Playwright.

    Methods:
        launch()
        close()
        open_page()
        close_page()
        get_responses()
        dropdown_values()

    Attributes:
        pages (list): List of all open pages.
        page_responses (dict): Network Responses grouped by page.
    """

    def __init__(self):
        """Initialize a new Browser instance."""
        self.pages = []
        self.page_responses = {}

    async def __aenter__(self):
        """Enter the asynchronous context and launch the browser."""
        await self.launch()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the asynchronous context and close the browser."""
        await self.close()

    async def launch(self, *, folder_user="browser", headless=True):
        """Launch the browser.

        Args:
            folder_user (str): User directory. Defaults to "browser".
            headless (bool): Launch browser in headless mode. Defaults to True.

        Returns:
            Browser: The browser instance.
        """
        self.playwright = await async_playwright().start()
        self.context = await self.playwright.chromium.launch_persistent_context(
            folder_user,
            headless=headless,
        )
        page = self.context.pages[0]
        self.pages.append(page)
        self._register_response_handler(page)
        return self

    async def close(self):
        """Close the browser."""
        await self.context.close()
        await self.playwright.stop()

    async def _newpage(self):
        """Create a new browser page."""
        page = await self.context.new_page()
        self.pages.append(page)
        self._register_response_handler(page)
        return page

    async def open_page(self, url, *, retries_max=3, timeout=5000, newtab=False):
        """Open a URL in a new or existing tab.

        Args:
            url (str): The URL to open.
            retries_max (int): Maximum number of retries. Defaults to 3.
            timeout (int): Page loading timeout in milliseconds. Defaults to 5000.
            newtab (bool): Whether to open the URL in a new tab. Defaults to False.

        Returns:
            int: Index of the page.
        """
        for _ in range(retries_max):
            page = await self._newpage() if newtab or not self.pages else self.pages[-1]
            try:
                await page.goto(url, timeout=timeout)
            except TimeoutError:
                continue
            else:
                return page
        msg = f"Failed to load the page after {retries_max} attempts."
        raise TimeoutError(msg)

    def get_page(self, page_index):
        """Get a page by its index."""
        try:
            return self.pages[page_index]
        except IndexError:
            msg = "Page index out of range."
            raise IndexError(msg) from None

    async def close_page(self, page_index):
        """Close a page and remove from the list.

        Args:
            page_index (int): Index of the page to close.
        """
        page = self.get_page(page_index)
        await page.close()
        self.pages.remove(page)
        if page in self.page_responses:
            del self.page_responses[page]

    def get_cookies(self):
        """Get Cookies for Browser Context."""
        return self.context.cookies()

    def _register_response_handler(self, page):
        """Register a handler to track page network responses."""

        async def handler(response):
            self.page_responses.setdefault(page, []).append(response)

        page.on("response", handler)

    def get_responses(self, page_index=0, url_filter=None):
        """Get network responses for a page.

        Args:
            page_index (int): Index of the page. Defaults to 0.
            url_filter (str): URL filter for responses. Optional.

        Returns:
            list: Network responses for the specified page.
        """
        page = self.get_page(page_index)
        responses = self.page_responses.get(page, [])
        if url_filter is not None:
            responses = [
                resp
                for resp in responses
                if all(key in resp.url() for key in url_filter)
            ]
        return responses

    async def dropdown_values(self, selector, page_index=0, element="option"):
        """Get dropdown values from a page.

        Args:
            selector (str): CSS Label of the dropdown.
            page_index (int): Index of the page. Defaults to 0.
            element (str): HTML tag name for value to select.

        Returns:
            list: List of dropdown values.
        """
        page = self.get_page(page_index)
        code_javascript = f"""
        () => {{
            const select = document.querySelector('{selector}');
            return Array.from(select.querySelectorAll('{element}'), option => option.value);
        }}
        """  # noqa: E501
        return await page.evaluate(code_javascript)


class Telegram:
    """A class for interacting with the Telegram using Bot URL.

    Methods:
        send_text()
        send_image()
        send_document()

    Attributes:
        bot_url (str): Base URL for the Telegram Bot API.
        sess (requests.Session): Session object for HTTP requests.
    """

    def __init__(self, bot_id):
        """Initialize the Telegram object with a bot ID.

        Args:
            bot_id (str): ID of the Telegram bot.
        """
        bot_id = bot_id[3:] if bot_id.startswith("bot") else bot_id
        self.bot_url = f"https://api.telegram.org/bot{bot_id}/"
        self.sess = requests.Session()

    def _post(self, endpoint: str, params=None, files=None):
        """Perform a POST request to a Telegram API endpoint."""
        response = self.sess.post(
            f"{self.bot_url}{endpoint}",
            params=params,
            files=files,
        )
        response.raise_for_status()

    def send_text(self, text, chat_id):
        """Send a text message to a Telegram chat.

        Args:
            text (str): Text to send.
            chat_id (int): Chat ID to send the message to.

        Returns:
            None
        """
        self._post(
            "sendMessage",
            params={"text": text, "chat_id": chat_id, "parse_mode": "html"},
        )

    def send_image(self, file_path, chat_id):
        """Send an image to a Telegram chat.

        Args:
            file_path (str): Path to the image file.
            chat_id (int): Chat ID to send the image to.

        Returns:
            None
        """
        file_path = make_path(file_path)
        with file_path.open("rb") as file_image:
            self._post(
                "sendPhoto",
                params={"chat_id": chat_id},
                files={"photo": file_image},
            )

    def send_document(self, file_path, chat_id):
        """Send a document to a Telegram chat.

        Args:
            file_path (str): Path to the document file.
            chat_id (int): Chat ID to send the document to.

        Returns:
            None
        """
        file_path = make_path(file_path)
        with file_path.open("rb") as file_document:
            self._post(
                "sendDocument",
                params={"chat_id": chat_id},
                files={"document": file_document},
            )


class AWS:
    """A class for handling Amazon Web Services operations.

    Methods:
        list_s3()
        upload_s3()
        download_s3()
        delete_s3()

    Attributes:
        client_s3 (boto3.resource): S3 client object.
    """

    def __init__(self, key_access, key_secret):
        """Initialize AWS Clients.

        Args:
            key_access (str): AWS access key.
            key_secret (str): AWS secret key.
        """
        self.client_s3 = boto3.resource(
            "s3",
            aws_access_key_id=key_access,
            aws_secret_access_key=key_secret,
        )

    def list_s3(self, bucket):
        """List all objects in a S3 bucket.

        Args:
            bucket (str): S3 bucket name.

        Returns:
            list: List of objects keys in the bucket.
        """
        return [obj.key for obj in self.client_s3.Bucket(bucket).objects.all()]

    def upload_s3(self, file_path, bucket, key):
        """Upload a file to a S3 bucket.

        Args:
            file_path (str): Local file path.
            bucket (str): S3 bucket name.
            key (str): Object key in the bucket.

        Returns:
            str: S3 URL of the uploaded file.
        """
        self.client_s3.Bucket(bucket).upload_file(
            Filename=make_path(file_path),
            Key=key,
        )
        return f"s3://{bucket}/{key}"

    def download_s3(self, file_path, bucket, key):
        """Download a file from a S3 bucket.

        Args:
            file_path (str): Local file path to save the download.
            bucket (str): S3 bucket name.
            key (str): Object key in the bucket.

        Returns:
            (str): Local file path of downloaded file.
        """
        self.client_s3.Bucket(bucket).download_file(
            Filename=make_path(file_path),
            Key=key,
        )
        return file_path

    def delete_s3(self, bucket, key):
        """Delete an object from a S3 bucket.

        Args:
            bucket (str): S3 bucket name.
            key (str): Object key in the bucket.

        Returns:
            None
        """
        self.client_s3.Object(bucket, key).delete()


class GCS:
    """A class for handling Google Cloud Services operations.

    Methods:
        list_gcs()
        upload_gcs()
        download_gcs()
        delete_gcs()

    Attributes:
        client_gcs (storage.Client):  client object.
    """

    def __init__(self):
        """Initialize GCS Clients."""
        self.client_gcs = storage.Client()

    def list_gcs(self, bucket):
        """List all objects in a GCS bucket.

        Args:
            bucket (str): GCS bucket name.

        Returns:
            list: List of objects keys in the bucket.
        """
        return [obj.name for obj in self.client_gcs.list_blobs(bucket)]

    def upload_gcs(self, file_path, bucket, key):
        """Upload a file to a GCS bucket.

        Args:
            file_path (str): Local file path.
            bucket (str): GCS bucket name.
            key (str): Object key in the bucket.

        Returns:
            str: GCS URL of the uploaded file.
        """
        self.client_gcs.bucket(bucket).blob(
            key).upload_from_filename(file_path)
        return f"gs://{bucket}/{key}"

    def download_gcs(self, file_path, bucket, key):
        """Download a file from a GCS bucket.

        Args:
            file_path (str): Local file path to save the download.
            bucket (str): GCS bucket name.
            key (str): Object key in the bucket.

        Returns:
            (str): Local file path of downloaded file.
        """
        self.client_gcs.bucket(bucket).blob(
            key).download_to_filename(file_path)
        return file_path

    def delete_gcs(self, bucket, key):
        """Delete an object from a GCS bucket.

        Args:
            bucket (str): GCS bucket name.
            key (str): Object key in the bucket.

        Returns:
            None
        """
        self.client_gcs.bucket(bucket).blob(key).delete()


class ACS:
    """A class for handling Azure Cloud Services operations.

    Methods:
        list_acs()
        upload_acs()
        download_acs()
        delete_acs()

    Attributes:
        client_acs (storage.Client): ACS client object.
    """

    def __init__(self, connection_string):
        """Initialize ACS Clients.

        Args:
            connection_string (str): ACS Connection string.
        """
        self.client_acs = BlobServiceClient.from_connection_string(
            connection_string)

    def list_acs(self, bucket):
        """List all objects in a ACS bucket.

        Args:
            bucket (str): ACS bucket name.

        Returns:
            list: List of objects keys in the bucket.
        """
        return [
            obj.name
            for obj in self.client_acs.get_container_client(bucket).list_blobs()
        ]

    def upload_acs(self, file_path, bucket, key):
        """Upload a file to a ACS bucket.

        Args:
            file_path (str): Local file path.
            bucket (str): ACS bucket name.
            key (str): Object key in the bucket.

        Returns:
            str: ACS URL of the uploaded file.
        """
        client_obj = self.client_acs.get_blob_client(
            container=bucket, blob=key)
        with make_path(file_path).open("rb") as data:
            client_obj.upload_blob(data)
        return client_obj.url

    def download_acs(self, file_path, bucket, key):
        """Download a file from a ACS bucket.

        Args:
            file_path (str): Local file path to save the download.
            bucket (str): ACS bucket name.
            key (str): Object key in the bucket.

        Returns:
            (str): Local file path of downloaded file.
        """
        client_obj = self.client_acs.get_blob_client(
            container=bucket, blob=key)
        with make_path(file_path).open("wb") as obj:
            obj.write(client_obj.download_blob().readall())
        return file_path

    def delete_acs(self, bucket, key):
        """Delete an object from a ACS bucket.

        Args:
            bucket (str): ACS bucket name.
            key (str): Object key in the bucket.

        Returns:
            None
        """
        self.client_acs.get_blob_client(
            container=bucket, blob=key).delete_blob()
