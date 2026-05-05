import asyncio
import random
import time
import httpx

# Configuration
BASE_URL = "http://localhost:8080"
CONCURRENCY = 20  # Number of parallel requests
TOTAL_REQUESTS = 500
ENDPOINTS = ["/pay", "/order"]

async def send_request(client, semaphore):
    async with semaphore:
        endpoint = random.choice(ENDPOINTS)
        start_time = time.time()
        try:
            response = await client.post(f"{BASE_URL}{endpoint}")
            latency = (time.time() - start_time) * 1000
            print(f"[{response.status_code}] {endpoint} - {latency:.2f}ms")
        except Exception as e:
            print(f"Request failed: {e}")

async def simulate_spike():
    print(f"🚀 Starting spike: {TOTAL_REQUESTS} requests with concurrency {CONCURRENCY}...")
    
    semaphore = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = [send_request(client, semaphore) for _ in range(TOTAL_REQUESTS)]
        await asyncio.gather(*tasks)
    
    print("\n✅ Spike simulation complete!")

if __name__ == "__main__":
    try:
        asyncio.run(simulate_spike())
    except KeyboardInterrupt:
        print("\nStopping simulation...")
