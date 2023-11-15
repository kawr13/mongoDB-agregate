from icecream import ic
import json
from mongoclient import aggregate_data
import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters.command import Command

logging.basicConfig(level=logging.INFO)


bot = Bot(token="6340822740:AAHdlCbrH5BdbDIqXAdwHb0Np6rIfGVgltM")

dp = Dispatcher()

@dp.message(Command('start'))
async def send_welcome(message: types.Message):
    """
    This handler will be called when user sends `/start` or `/help` command
    """
    await message.answer("Привет!\nЯ бот, и я умею делать агрегацию данных.")



@dp.message()
async def echo(message: types.Message):
   data = json.loads(message.text)
   dt_from = data["dt_from"]
   dt_upto = data["dt_upto"]
   group_type = data["group_type"]
   """
   Echo the user message
   """
   data = await aggregate_data(dt_from, dt_upto, group_type)
   await message.answer(str(data))



async def main():
   await dp.start_polling(bot, skip_updates=True)




if __name__ == '__main__':
   asyncio.run(main())
   print('end')
