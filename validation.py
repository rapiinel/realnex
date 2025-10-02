api_key = 'cb527605-7b5e-460f-a8d8-edecfed8b847'

import pandas as pd
import asyncio
import aiohttp
import time
from aiohttp import ClientSession
from typing import List
import nest_asyncio
nest_asyncio.apply()

API_URL = "https://api.phonevalidator.com/api/v3/phonesearch"

async def fetch_line_type(session: ClientSession, phone: str, api_key: str) -> str:
    params = {
        "apikey": api_key,
        "phone": phone,
        "type": "basic"
    }
    try:
        async with session.get(API_URL, params=params, timeout=10) as resp:
            data = await resp.json()
            return data.get("PhoneBasic", {}).get("LineType", "Unknown")
    except Exception as e:
        return f"Error: {e}"

async def process_batch(phones: List[str], api_key: str) -> List[str]:
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_line_type(session, phone, api_key) for phone in phones]
        return await asyncio.gather(*tasks)

def run_rate_limited_requests(df: pd.DataFrame, phone_col: str, api_key: str = api_key, rate_limit: int = 25) -> pd.DataFrame:
    phones = df[phone_col].tolist()
    results = []

    for i in range(0, len(phones), rate_limit):
        batch = phones[i:i + rate_limit]
        start = time.time()
        batch_result = asyncio.run(process_batch(batch, api_key))
        results.extend(batch_result)
        elapsed = time.time() - start

        # throttle to 1 batch per second
        sleep_time = max(0, 1 - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)

    df["line_type"] = results
    return df


if __name__ == '__main__':
    for_phone_validation = pd.read_csv("File Directory")
    for_phone_validation.rename(columns={'value':'phone'}, inplace=True)
    phone_numbers_result = run_rate_limited_requests(for_phone_validation, "phone", api_key=api_key, rate_limit=20)

    print(phone_numbers_result)
