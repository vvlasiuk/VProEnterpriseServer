from app.models.catalog import Catalog
from app.models.models_catalog.catalog_schemas_DTO import CatalogProductBrandDTO

class Cat_ProductBrand(Catalog):
    # __slots__ = ()
    _db_head = {"table_name": "cat_products_brands", "columns": ["name", "mark_deleted", "_created_by"]}
    # _db_head_table_name = "cat_products_brands"
    # _db_head_table_columns = ["name", "mark_deleted"]
    _db_tables = {
        "table_one": {"table_name": "cat_products_brands", "columns": ["name", "mark_deleted"]},
        "table_two": {"table_name": "cat_products_brands", "columns": ["name", "mark_deleted"]}
    }

    def __init__(self):
        super().__init__()
        self.head: CatalogProductBrandDTO = None
        self.table_one: CatalogProductBrandDTO = None  
        self.table_two: CatalogProductBrandDTO = None

        # self.head: CatalogBaseDTO = CatalogBaseDTO()  

    # async def save(self):

    #     columns = ', '.join(data.keys())
    #     placeholders = ', '.join(['?'] * len(data))
    #     sql = f"INSERT INTO {self.db_head_table_name} ({columns}) VALUES ({placeholders})"
    #     async with db_manager.get_transaction() as cursor:
    #         await cursor.execute(sql, tuple(data.values()))
    #         return True
    
    @classmethod
    def new(cls):
        obj = cls()
        obj.head = CatalogProductBrandDTO()
        obj.table_one = CatalogProductBrandDTO()
        obj.table_two = CatalogProductBrandDTO()
        return obj
    
    @classmethod
    async def get_by_id(cls, item_id):
        row_dict = await super().get_by_id(item_id)
        if row_dict:
            obj = cls()
            # Заповнюємо head через дата-клас
            obj.head = CatalogProductBrandDTO(**row_dict)
            return obj
        return None
