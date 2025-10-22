import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
from app.models.models_catalog.cat_products_brands import Cat_ProductBrand
from app.models.models_catalog.cat_external_source import Cat_ExternalSource
# from app.models.models_catalog.cat_external_data_dto import Cat_ExternalData
from app.db.database import db_manager
import random
import string

async def main():
    await db_manager.create_pool()

    # source = Cat_ExternalSource.new()
    # source.head.name = "CI 1c8"
    # source.head.is_active = True
    # source.head._created_by = 1
    # source_save = await source.save()

    # ext_data = Cat_ExternalData.new()
    # ext_data.head.external_source_id = 1
    # ext_data.head.external_id = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
    # ext_data.head.internal_id = 1
    # ext_data.head.internal_typeid = 6
    # # await Cat_ProductBrand.get_head_typeid()
    # ext_data_save = await ext_data.save()

    # brand = Cat_ProductBrand.new()
    # brand.head.name = ''.join(random.choices(string.ascii_letters, k=8))
    # brand.head.mark_deleted = random.choice([True, False])
    # brand.head._created_by = 1
    # brand.head.external_id = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
    # brand.head.external_source_id = 1
    # brand_save = await brand.save()


    # brand_1 = await Cat_ProductBrand.get_by_id(1)
    # print(brand_1.head)

    # brand = await Cat_ProductBrand.get_by_external_id("JIho2P0hyleT", 1)
    # if not brand:
    #     brand = Cat_ProductBrand.new()
    #     brand.head._created_by = 1
    #     brand.head.external_id = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
    #     brand.head.external_source_id = 1

    # brand.head.name = ''.join(random.choices(string.ascii_letters, k=8))
    # brand.head.mark_deleted = random.choice([True, False])
    # brand_save = await brand.save()
    # print(brand_save.head)

    # brand_2 = await Cat_ProductBrand.get_by_external_id("JIho2P0hyleT", 1)
    # print(brand_1.head)

    # head_typeid = await Cat_ProductBrand.get_head_typeid()
    # print(f"Head TypeID: {head_typeid}")
    
    await db_manager.close_pool()

import asyncio
asyncio.run(main())