import asyncio
import aio_pika
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
import os
from dotenv import load_dotenv
from aiogram.exceptions import TelegramRetryAfter
import logging
import random

load_dotenv()

TOKEN = os.getenv("TOKEN")
bot = Bot(token=TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
RATE_LIMIT=1/30

RABBITMQ_HOST = os.getenv("HOST")
QUEUE_NAME = 'priority_queue'

workers = 5  

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

async def send_message_worker(sender: MessageSender, chat_id, message_text):
    await sender.send_message(chat_id, message_text)

async def create_queue():
    connection = await aio_pika.connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()
    queue = await channel.declare_queue(
        QUEUE_NAME, durable=True, arguments={'x-max-priority': 10}
    )
    return queue, connection

async def send_to_queue(message_text: str, chat_id: int, priority: int):
    connection = await aio_pika.connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=f'{message_text},{chat_id}'.encode(),
            priority=priority
        ), routing_key=QUEUE_NAME
    )
    await connection.close()

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

@dp.message(Command('send'))
async def handle_send(message: Message):
    for i in range(100):
        await send_to_queue(f"Test message {i + 1}", message.chat.id, priority=5)
    await message.answer("messages're in queue")

async def main():
    queue, connection = await create_queue()
    senders = [MessageSender(bot, mock=False) for _ in range(2)] 

    tasks = []
    for sender in senders:
        tasks.append(process_queue(queue, sender))
    await asyncio.gather(
        dp.start_polling(bot),
        *[asyncio.sleep(1/30) for _ in range(90)], 
        *tasks,
    )

if __name__ == '__main__':
    asyncio.run(main())
