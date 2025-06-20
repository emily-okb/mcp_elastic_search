import requests, time
import tqdm
import aiohttp
import asyncio


async def fetch_sparse_vec(session, batch_text):
    data = {
        "inputs": batch_text,
    }
    for _ in range(1):
        async with session.post("http://10.12.160.9:8084/embed_sparse", json=data) as resp:
            if resp.status == 200:
                result = await resp.json()
                if result:  # Check if the result is empty
                    return result
                else:
                    print("Received empty response")
                    return None
            elif resp.status == 413:
                print("Error 413")
                await asyncio.sleep(0.2)
            else:
                print(f"Error: Received status code {resp.status}")
                return None
    return None


async def call_sparse_vec_api(batch_text, batch_size=128):
    async with aiohttp.ClientSession() as session:
        tasks = []
        # Split the text into smaller batches and create tasks
        for batch in batch_split(batch_text, batch_size):
            tasks.append(fetch_sparse_vec(session, batch))
        
        results = await asyncio.gather(*tasks)  # Execute all requests concurrently
        return [result for result in results if result is not None]


def batch_split(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


if __name__ == "__main__":

    ret = call_sparse_vec_api(["This is a test document.", "Another test document for sparse vector enrichment."])
    print(ret)