import asyncio
import aio_pika
from aiogram import Bot, Dispatcher
from aiogram.types import Message
import os
from dotenv import load_dotenv
import random
import uvicorn
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from aiogram.exceptions import TelegramRetryAfter
import logging

load_dotenv()

TOKEN = os.getenv("TOKEN")
bot = Bot(token=TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
RATE_LIMIT = 1 / 30

RABBITMQ_HOST = os.getenv("HOST", "amqp://guest:guest@localhost/")
QUEUE_NAME = 'priority_queue'

workers = 5

templates = Jinja2Templates(directory="templates") 

class MessageSender:
    def __init__(self, bot, mock=False):
        self.bot = bot
        self.mock = mock
    
    async def send_message(self, chat_id, message_text):
        while True:
            try:
                if self.mock:
                    logger.info(f"Mock send: {message_text} to chat {chat_id}")
                    await asyncio.sleep(random.uniform(0.1, 0.5)) 
                else:
                    await self.bot.send_message(chat_id, message_text)
                await asyncio.sleep(RATE_LIMIT) 
                break
            except TelegramRetryAfter as e:
                retry_seconds = getattr(e, "retry_after", 5)
                logger.warning(f"Flood control: retry in {retry_seconds} секунд...")
                await asyncio.sleep(retry_seconds)
            except Exception as e:
                logger.error(f"error: {e}")
                break

async def connect_to_rabbitmq():
    while True:
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_HOST)
            channel = await connection.channel()
            return connection, channel
        except Exception as e:
            print(f"Error connecting to RabbitMQ: {e}. Retrying...")
            await asyncio.sleep(5)

async def send_to_queue(message_text: str, chat_id: int, priority: int):
    connection, channel = await connect_to_rabbitmq()
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=f'{message_text},{chat_id}'.encode(),
            priority=priority
        ), routing_key=QUEUE_NAME
    )
    await connection.close()

async def send_message_worker(sender: MessageSender, chat_id, message_text):
    await sender.send_message(chat_id, message_text)

async def create_queue():
    connection, channel = await connect_to_rabbitmq()
    await channel.declare_queue(
        QUEUE_NAME, durable=True, arguments={'x-max-priority': 10}
    )
    queue = await channel.get_queue(QUEUE_NAME)
    return connection, queue

async def process_queue(queue, sender):
    async def message_handler(message: aio_pika.IncomingMessage):
        async with message.process():
            try:
                message_data = message.body.decode('utf-8').split(',')
                message_text = message_data[0]
                chat_id = int(message_data[1])
                await send_message_worker(sender, chat_id, message_text)
            except Exception as e:
                logger.error(f"error: {e}")
    
    await queue.consume(message_handler)

app = FastAPI()

@app.get("/", response_class=HTMLResponse)
async def get_form(request: Request):
    return templates.TemplateResponse("form.html", {"request": request})

@app.post("/send")
async def send_message(
    chat_id: int = Form(...),
    message_text: str = Form(...),
    num_messages: int = Form(...),
):
    for _ in range(num_messages):
        await send_to_queue(message_text, chat_id, priority=5)
    return {"message": "Messages are in queue"}

async def start_services():
    connection, queue = await create_queue()
    senders = [MessageSender(bot, mock=False) for _ in range(workers)]

    tasks = []
    for sender in senders:
        tasks.append(process_queue(queue, sender))
    await asyncio.gather(*tasks)

async def main():
    loop = asyncio.get_event_loop()
    await start_services()
    await loop.run_in_executor(None, lambda: uvicorn.run(app, host="127.0.0.1", port=8000))

if __name__ == '__main__':
    asyncio.run(main())
