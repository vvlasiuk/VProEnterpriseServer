from ast import Dict
from app.models.models_catalog.catalog import Catalog
from app.models.models_catalog.catalog_schemas_dto import CatalogExternalDataDTO

class Cat_ExternalData(Catalog):
    _db_head = {"table_name": "cat_external_data", "columns": ["external_source_id", "external_id", "internal_id", "internal_typeid"]}    

    def __init__(self):
        super().__init__()
        self.head: CatalogExternalDataDTO = None
    
    def get_db_head_structure() -> Dict:
        return {
            'table_name': 'cat_external_data',
            'description': 'Звязок з зовнішніми даними',
            'columns': {                
                '_id': {
                    'description': 'УІ', 
                    'comment': 'Унікальний ідентифікатор',
                    'type': 'BIGINT',
                    'primary_key': True,
                    'auto_increment': True,
                    'nullable': False
                },
                'external_source_id': {
                    'description': 'Джерело даних',
                    'type': 'BIGINT',
                    'nullable': False,
                    'foreign_key': 'cat_external_sources._id'
                    },
                'external_id': {
                    'description': 'Зовнішній ID/артикул/код',
                    'type': 'NVARCHAR(50)',
                    'nullable': False
                },
                'internal_id': {
                    'description': 'Внутрішній ідентифікатор',
                    'comment': 'ID запису в нашій БД',
                    'type': "BIGINT",
                    'nullable': True,
                },
                'internal_typeid': {
                    'description': 'Тип даних внутрішнього ідентифікатора',
                    'type': "BIGINT",
                    'nullable': True,
                    'foreign_key': 'sys_data_types.id'
                },
                '_created_at': {
                    'description': 'Дата створення звязку',
                    'type': 'DATETIME2',
                    'default': 0,
                    'default': "GETDATE()"
                }
            }
        }