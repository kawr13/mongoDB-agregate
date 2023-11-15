from datetime import datetime
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
import asyncio
import motor.motor_asyncio
from icecream import ic


async def connect_to_mongodb():
    try:
        client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/')
        db = client['mydb']
        collection = db['sample_collection']
        return collection
    except Exception as e:
        print("Ошибка подключения к MongoDB:", str(e))
        return None



async def aggregates(current_date, next_date, collection):
    try:
        pipeline = [
            {
                "$match": {
                    "dt": {"$gte": current_date, "$lt": next_date}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total": {"$sum": '$value'}
                }
            }
        ]
        cursor = collection.aggregate(pipeline)
        result = await cursor.to_list(length=None)
        return result
    except Exception as e:
        ic("Ошибка выполнения агрегации данных:", str(e))
        return None


async def aggregate_data(dt_from, dt_upto, group_type):
    collection = await connect_to_mongodb()
    if collection is None:
        return None
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)
    dataset = []
    labels = []
    if group_type == 'hour':
        interval = relativedelta(hours=1)
    elif group_type == 'day':
        interval = relativedelta(days=1)
    elif group_type == 'month':
        interval = relativedelta(months=1)
    else:
        raise ValueError("Недопустимый тип")
    current_date = dt_from
    while current_date < dt_upto:
        next_date = current_date + interval
        result = await aggregates(current_date, next_date, collection)
        ic(result)
        if result:
            dataset.append(result[0].get("total", 0))
        else:
            dataset.append(0)
        labels.append(current_date.isoformat())
        current_date = next_date
    return {"dataset": dataset, "labels": labels}


if __name__ == '__main__':
    dt_from = "2022-02-01T00:00:00"
    dt_upto = "2022-02-02T00:00:00"
    group_type = "hour"



