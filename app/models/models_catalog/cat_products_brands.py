from app.models.models_catalog.catalog import Catalog
from app.models.models_catalog.catalog_schemas_dto import CatalogProductBrandDTO

class Cat_ProductBrand(Catalog):
    _DTO = CatalogProductBrandDTO
    # _typeid = None
    _db_head = Catalog._db_head.copy()
    _db_head["table_name"] = "cat_products_brands"
    # _db_head["table_typeid"] = Catalog.get_head_typeid(_db_head["table_name"])

    _db_tables = {
        "table_one": {"table_name": "cat_products_brands", "columns": ["name", "mark_deleted"]},
        "table_two": {"table_name": "cat_products_brands", "columns": ["name", "mark_deleted"]}
    }
    
    def __init__(self):
        super().__init__()
        self.head: CatalogProductBrandDTO = None
        self.table_one: CatalogProductBrandDTO = None  
        self.table_two: CatalogProductBrandDTO = None
    
    # @classmethod
    # def new(cls):
    #     obj = cls()
    #     obj.head = CatalogProductBrandDTO()
    #     obj.table_one = CatalogProductBrandDTO()
    #     obj.table_two = CatalogProductBrandDTO()
        
    #     return obj
    
    # @classmethod
    # async def get_by_id(cls, item_id):
    #     row_dict = await super().get_by_id(item_id)
    #     if row_dict:
    #         obj = cls()
    #         # Заповнюємо head через дата-клас
    #         obj.head = CatalogProductBrandDTO(**row_dict)
    #         return obj
    #     return None

    # @classmethod
    # async def get_by_external_id(cls, external_id: str, source_id: int):
    #     row_dict = await super().get_by_external_id(external_id, source_id)
    #     if row_dict:
    #         obj = cls()
    #         # Заповнюємо head через дата-клас
    #         # obj.head = CatalogProductBrandDTO(**row_dict)
    #         obj.head = obj._DTO(**row_dict)
    #         return obj
    #     return None

    # @classmethod
    # async def get_head_typeid(cls):
    #     return await super().get_head_typeid(cls._db_head["table_name"])
