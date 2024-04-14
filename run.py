import aiohttp
import aiofiles
import aiohttp.client_exceptions
import pandas as pd
import asyncio
import json
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, FSInputFile
import sys
from bs4 import BeautifulSoup
from loguru import logger
out = []
YEARS = [2024-x for x in range(1, 8)]
FGIS_ENDPOINT = "https://fgis.gost.ru/fundmetrology/eapi/vri"
TOKEN = "7168554651:AAFrR-3dp1XvY9ZiUw9GZlYVccYxzkqZuRw"
bot = Bot(TOKEN)
dp = Dispatcher()
tasks: list[asyncio.Task] = []
class ProxieManager:
    updater_tasks = []
    def __init__(self) -> None:
        self.proxies = [ {"busy": False, "proxy":"http://"+x.strip("\n") } for x in open("proxies.txt").readlines()]

    async def update_proxy(self, proxy_id):
        await asyncio.sleep(2)
        self.proxies[proxy_id]["busy"] = False

    async def __call__(self):
        while True:
            for idx, proxy in enumerate(self.proxies):
                if not proxy["busy"]:
                    self.updater_tasks.append(asyncio.create_task(
                            self.update_proxy(idx)
                        ))
                    proxy["busy"] = not proxy["busy"]
                    return proxy["proxy"]
            await asyncio.gather(*self.updater_tasks)

pm = ProxieManager()
        
async def collect_data(year: int, mit_number: str):
    session = aiohttp.ClientSession()
    out = []

    def unwrap_html(raw_html_data: str):
        bs = BeautifulSoup(raw_html_data, features="html.parser")
        return bs.get_text()

    async def get_small_data(rows: int = 100, start: int = 0):
        while True:
            try:
                raw = await session.get(
                    FGIS_ENDPOINT,
                    params={
                        "mit_number": mit_number,
                        "year": year,
                        "rows": rows,
                        "start": start,
                    },
                    proxy = await pm()
                )
                response_json = await raw.json()
                break
            except aiohttp.client_exceptions.ContentTypeError:
                continue
        
        result = response_json["result"]["items"]
        if response_json["result"]["count"] > rows + start:
            logger.info(f"{response_json["result"]["count"] - (rows+start)} remaining ({round((response_json["result"]["count"] - (rows+start))/100)} s)")
            result += await get_small_data(start=start + rows)

        return result

    async def get_full_data(vri_ids: list[str]):
        fulldata_tasks = set()
        for idx, vri_id in enumerate(vri_ids):
            try:
                fulldata_tasks.add(asyncio.create_task(session.get(FGIS_ENDPOINT + "/" + vri_id, proxy=await pm())))
                logger.info(
                    f"{idx+1}. VRI ID: {vri_id} handled. {len(vri_ids) - (idx+1)} remains. ({len(vri_ids) - (idx+1)} s)"
                )
            except:
                global out
                df = pd.DataFrame(out)
                df.to_excel("disconnected_error_output.xlsx")
                pass

        if any(fulldata_tasks):
            result = await asyncio.wait(fulldata_tasks, return_when=asyncio.ALL_COMPLETED)
            for x in result[0]:
                for i in range(5):
                    try:
                        yield await x.result().json()
                        break
                    except Exception:
                        pass


                

    async def format_output(result):
        fields = {"Владелец СИ" : ["vriInfo", "miOwner"],
               "Регистрационный номер СИ": ["miInfo", "singleMI", "mitypeNumber"],
               "Тип СИ": ["miInfo", "singleMI", "mitypeType"],
               "Наименование типа СИ": ["miInfo", "singleMI", "mitypeTitle"],
               "Заводской номер СИ": ["miInfo", "singleMI", "manufactureNum"],
                "Модификация СИ": ["miInfo", "singleMI", "modification"],
               "Наименование организации-поверителя": ["vriInfo", "organization"],
               "Дата поверки": ["vriInfo", "vrfDate"],
               "Поверка дейсвительна до": ["vriInfo", "validDate"]}
        out = []
        for x in result:
            node = {}
            for key in fields.keys():
                value = x
                try:
                    for inner_key in fields[key]:
                        value = value[inner_key]
                    node[key] = value 
                except KeyError as err:
                    node[key] = "Нет данных"
            out.append(node)
        return out

    data = await get_small_data()
    vri_ids = [x["vri_id"] for x in data]
    await asyncio.sleep(1)
    result = await format_output([x["result"] async for x in get_full_data(vri_ids)])
    await session.close()
    return result

async def main():
    global out
    await dp.start_polling(bot)
    
@dp.message(F.text)
async def handle_message(message: Message):
    tasks = []
    message_lines = message.text.split("\n")
    year = int(message_lines[0])
    if year not in YEARS:
        year = None
        mit_numbers = message_lines[0:]
        for mit_number in mit_numbers:
            for year in YEARS:
                tasks.append(
                    asyncio.create_task(
                        collect_data(year, mit_number)
                    )
                )
    else:
        mit_numbers = message_lines[1:]
        for mit_number in mit_numbers:
                tasks.append(
                    asyncio.create_task(
                        collect_data(year, mit_number)
                    )
                )
    


    res = (await asyncio.wait(tasks))[0].pop().result()
    out = res
    df = pd.DataFrame(out)
    df.to_excel("out.xlsx")
    await message.answer_document(FSInputFile("out.xlsx"))
    logger.info("All Done!")

async def test1():
    result = await collect_data(2021, "38902-15")
    df = pd.DataFrame(result)
    df.to_excel("out.xlsx")

    logger.info("All Done!")

async def test2():
    result = []
    file = "nums.txt"
    for year in YEARS:
        async with aiofiles.open(file) as mit_n_f:
            mit_numbers = await mit_n_f.readlines()
            mit_numbers = [x.strip() for x in mit_numbers]
            logger.info(f"Номера СИ загружены:\n{"\n".join(mit_numbers)}")
        for mit_number in mit_numbers:
            logger.info(f"Обрабатываю номер {mit_number}")
            result += await collect_data(year, mit_number)
            await asyncio.sleep(1)
        await asyncio.sleep(1)
    df = pd.DataFrame(result)
    df.to_excel("out.xlsx")


asyncio.run(main())
