import json
import logging
from json import JSONDecodeError
from typing import Any

import aiohttp
import asyncio

from hyperliquid.utils.constants import MAINNET_API_URL
from hyperliquid.utils.error import ClientError, ServerError


class API:
    def __init__(self, base_url=None):
        self.base_url = MAINNET_API_URL
        self.headers = {
            "Content-Type": "application/json",
        }

        if base_url is not None:
            self.base_url = base_url

        self._logger = logging.getLogger(__name__)

    async def post(self, url_path: str, payload: Any = None) -> Any:
        if payload is None:
            payload = {}
        url = self.base_url + url_path

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=self.headers) as response:
                await self._handle_exception(response)

                try:
                    return await response.json()
                except aiohttp.ContentTypeError:
                    text = await response.text()
                    return {"error": f"Could not parse JSON: {text}"}

    async def _handle_exception(self, response):
        status_code = response.status
        if status_code < 400:
            return
        if 400 <= status_code < 500:
            try:
                err = await response.json()
            except JSONDecodeError:
                text = await response.text()
                raise ClientError(status_code, None, text, None, response.headers)
            error_data = err.get("data", None)
            raise ClientError(status_code, err["code"], err["msg"], response.headers, error_data)
        text = await response.text()
        raise ServerError(status_code, text)
