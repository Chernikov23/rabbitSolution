import asyncio
import aio_pika
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import time
import random

load_dotenv()

TOKEN = os.getenv("TOKEN")
bot = Bot(token=TOKEN)
dp = Dispatcher()
RABBITMQ_HOST = os.getenv('HOST')
QUEUE_NAME = 'priority_queue'
NUM_WORKERS = 5 

class TelegramSender:
    def __init__(self, token):
        self.bot = Bot(token)
    async def send_message(self, chat_id, message_text):
        await self.bot.send_message(chat_id, message_text)

async def send_message_worker(sender: TelegramSender, chat_id, message_text):
    await sender.send_message(chat_id, message_text)


async def create_queue():
    connection = await aio_pika.connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()
    queue = await channel.declare_queue('priority_queue', durable=True, arguments={'x-max-priority': 10})
    return queue, connection


async def send_to_queue(message_text: str, chat_id: int, priority: int):
    connection = await aio_pika.connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=f'{message_text},{chat_id}'.encode(),
            priority=priority
        ), routing_key='priority_queue'
    )
    await connection.close()

async def process_queue(queue, sender):
    async def message_handler(message: aio_pika.IncomingMessage):
        async with message.process():
            message_data = message.body.decode('utf-8').split(',')
            message_text = message_data[0]
            chat_id = int(message_data[1])
            await send_message_worker(sender, chat_id, message_text)

    await queue.consume(message_handler)

@dp.message(Command('send'))
async def handle_send(message: Message):
    for i in range(70):
        await send_to_queue(f"Test message {i + 1}", message.chat.id, priority=5)
    await message.answer("Messages are being sent...")

async def main():
    queue, connection = await create_queue()
    senders = [TelegramSender(TOKEN) for _ in range(NUM_WORKERS)]
    tasks = []
    for sender in senders:
        tasks.append(process_queue(queue, sender))
    await asyncio.gather(*tasks)
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
