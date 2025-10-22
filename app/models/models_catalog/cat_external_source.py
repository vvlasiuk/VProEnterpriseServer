from app.models.models_catalog.catalog import Catalog
from app.models.models_catalog.catalog_schemas_dto import CatalogExternalSourceDTO

class Cat_ExternalSource(Catalog):
    # _db_head = {"table_name": "cat_external_sources", "columns": ["is_active", "last_sync_at"]} 
    _db_head = Catalog._db_head.copy()
    _db_head["table_name"] = "cat_external_sources"
    _db_head["columns"] = Catalog._db_head["columns"] + ["is_active", "last_sync_at"]

    def __init__(self):
        super().__init__()
        self.head: CatalogExternalSourceDTO = None
    
    @classmethod
    def new(cls):
        obj = cls()
        obj.head = CatalogExternalSourceDTO()
        return obj
    
    @classmethod
    async def get_by_id(cls, item_id):
        row_dict = await super().get_by_id(item_id)
        if row_dict:
            obj = cls()
            # Заповнюємо head через дата-клас
            obj.head = CatalogExternalSourceDTO(**row_dict)
            return obj
        return None

    # @classmethod
    # async def get_head_typeid(cls):
    #     return await super().get_head_typeid(cls._db_head["table_name"])
