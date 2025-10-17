import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
from app.models.models_catalog.products_brands_DTO import Cat_ProductBrand
from app.db.database import db_manager
import random
import string

async def main():
    await db_manager.create_pool()

    brand = Cat_ProductBrand.new()
    brand.head.name = ''.join(random.choices(string.ascii_letters, k=8))
    brand.head.mark_deleted = random.choice([True, False])
    brand.head._created_by = 1
    brand_save = await brand.save()

    brand_1 = await Cat_ProductBrand.get_by_id(brand_save)
    print(brand_1.head)

    await db_manager.close_pool()

import asyncio
asyncio.run(main())