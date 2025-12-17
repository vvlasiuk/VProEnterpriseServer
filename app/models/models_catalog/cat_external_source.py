from ast import Dict
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

    def get_db_head_structure() -> Dict:
        structure = Catalog.get_db_head_structure()
        structure['table_name'] = "cat_external_sources"
        structure['description'] = "Зовнішні джерела даних"
        structure['subsystem'] = "import_export"
        
        # Додаємо власні колонки
        structure['columns'].update({
            'is_active': {
                'description': 'Активне джерело',
                'type': 'BIT',
                'nullable': False,
                'default': 1
            },
            'last_sync_at': {
                'description': 'Дата останньої синхронізації',
                'type': 'DATETIME2',
                'nullable': True
            }
        })
        
        return structure