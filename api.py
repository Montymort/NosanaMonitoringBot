import logging
import asyncio
import aiohttp
import schedule
import time
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Read sensitive data from environment variables
TOKEN = os.getenv("TOKEN")  # Telegram Bot Token
CHAT_ID = os.getenv("CHAT_ID")  # Telegram Chat ID
NODE_ADDRESSES = os.getenv("NODE_ADDRESSES")  # Comma-separated list of Node Addresses

# Verify environment variables
if not TOKEN or not CHAT_ID or not NODE_ADDRESSES:
    raise EnvironmentError("TOKEN, CHAT_ID, or NODE_ADDRESSES is missing in .env file.")

# Parse node addresses into a list
NODE_ADDRESSES = NODE_ADDRESSES.split(',')

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Store the last known states and message_ids
last_known_states = {node: None for node in NODE_ADDRESSES}
last_message_ids = {node: None for node in NODE_ADDRESSES}

# Function to send message via Telegram
async def send_message_async(message, node_address):
    global last_message_ids
    try:
        async with aiohttp.ClientSession() as session:
            url = f'https://api.telegram.org/bot{TOKEN}/sendMessage'
            params = {
                'chat_id': CHAT_ID,
                'text': message,
                'parse_mode': 'Markdown'
            }
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                result = await response.json()
                last_message_ids[node_address] = result['result']['message_id']
                logger.debug(f"Telegram response for {node_address}: {result}")
    except Exception as e:
        logger.error(f"Failed to send message for {node_address}: {e}")

# Function to edit a message via Telegram
async def edit_message_async(message, node_address):
    global last_message_ids
    try:
        async with aiohttp.ClientSession() as session:
            url = f'https://api.telegram.org/bot{TOKEN}/editMessageText'
            params = {
                'chat_id': CHAT_ID,
                'message_id': last_message_ids[node_address],
                'text': message,
                'parse_mode': 'Markdown'
            }
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                result = await response.json()
                logger.debug(f"Telegram edit response for {node_address}: {result}")
    except Exception as e:
        logger.error(f"Failed to edit message for {node_address}: {e}")

# Monitor the API for state changes
async def monitor_api(node_address):
    try:
        url = f'https://{node_address}.node.k8s.prd.nos.ci/node/info'
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                state = data.get('state', 'UNKNOWN')
                uptime_str = data.get('uptime', 'UNKNOWN')
                return state, uptime_str
    except aiohttp.ClientError as e:
        logger.error(f"Error checking API for {node_address}: {e}")
        return "ERROR", None

# Function to calculate uptime
def calculate_uptime(uptime_str):
    try:
        uptime_time = datetime.strptime(uptime_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        now = datetime.utcnow()
        delta = now - uptime_time
        return str(delta).split('.')[0]
    except ValueError:
        return "Invalid uptime format"

# Send the current node state to Telegram
async def send_current_node_state(node_address):
    global last_known_states
    state, uptime_str = await monitor_api(node_address)
    uptime = calculate_uptime(uptime_str) if uptime_str else "Unknown"
    display_state = "JOB/OTHER" if state == "OTHER" else state

    if state == "ERROR":
        message = (
            f"⚠️ *Node Status Alert!*\n"
            f"💻 *Node:* `{node_address}`\n"
            f"❌ *Status:* API is not responding.\n"
            f"🛠 Please check the node for issues!"
        )
        await send_message_async(message, node_address)
    elif state != last_known_states[node_address]:
        message = (
            f"🔄 *Node State Change Detected!*\n"
            f"💻 *Node:* `{node_address}`\n"
            f"🌐 *New State:* `{display_state}`\n"
            f"🕒 *Uptime:* `{uptime}`\n"
            f"🚀 *Status updated successfully!*"
        )
        await send_message_async(message, node_address)
    else:
        message = (
            f"⏳ *Node Status Update:*\n"
            f"💻 *Node:* `{node_address}`\n"
            f"🌐 *State:* `{display_state}`\n"
            f"🕒 *Uptime:* `{uptime}`\n"
            f"📈 Monitoring continues!"
        )
        if last_message_ids[node_address]:
            await edit_message_async(message, node_address)
        else:
            await send_message_async(message, node_address)

    last_known_states[node_address] = state
    logger.debug(f"Last known state for {node_address} updated to: {state}")

async def send_initial_message(node_address):
    message = (
        f"🚀 *Node Monitor Activated!*\n"
        f"💻 *Monitoring Node:* `{node_address}`\n"
        f"📡 *System checks running every 30 seconds.*"
    )
    await send_message_async(message, node_address)

def job():
    asyncio.run(monitor_nodes())

async def monitor_nodes():
    tasks = [send_current_node_state(node) for node in NODE_ADDRESSES]
    await asyncio.gather(*tasks)

# Schedule the job to run every 30 seconds
schedule.every(30).seconds.do(job)

# Initialize messages for all nodes
async def initialize_nodes():
    tasks = [send_initial_message(node) for node in NODE_ADDRESSES]
    await asyncio.gather(*tasks)

asyncio.run(initialize_nodes())

while True:
    schedule.run_pending()
    time.sleep(1)
