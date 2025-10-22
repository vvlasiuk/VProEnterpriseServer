from app.models.models_catalog.catalog import Catalog
from app.models.models_catalog.catalog_schemas_dto import CatalogExternalDataDTO

class Cat_ExternalData(Catalog):
    _db_head = {"table_name": "cat_external_data", "columns": ["external_source_id", "external_id", "internal_id", "internal_typeid"]}    

    def __init__(self):
        super().__init__()
        self.head: CatalogExternalDataDTO = None
    
    # @classmethod
    # def new(cls):
    #     obj = cls()
    #     obj.head = CatalogExternalDataDTO()
    #     return obj
    
    # @classmethod
    # async def get_by_id(cls, item_id):
    #     row_dict = await super().get_by_id(item_id)
    #     if row_dict:
    #         obj = cls()
    #         # Заповнюємо head через дата-клас
    #         obj.head = CatalogExternalDataDTO(**row_dict)
    #         return obj
    #     return None

    # @classmethod
    # async def get_head_typeid(cls):
    #     return await super().get_head_typeid(cls._db_head["table_name"])
