from app.models.models_catalog.catalog import Catalog
from app.models.models_catalog.catalog_schemas_dto import CatalogProductBrandDTO
from app.utils.converters import value_to_bool_bit

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

    @classmethod
    def import_from_rows_prepare(rows: list):
        for row in rows:
            row['Mark_deleted'] = value_to_bool_bit(row.get('Mark_deleted'))
    
    @classmethod
    async def import_from_rows(cls, rows: list, source_id: int, user_id: int):
        
        cls.import_from_rows_prepare(rows)

        result = []
        for row in rows:
            brand = await cls.get_by_external_id(row.get('External_ID'), source_id)
            if brand:
                brand.head.name = row.get('Name')
                brand.head.mark_deleted = row.get('Mark_deleted', 0)
            else:
                brand = cls.new()
                brand.head.name = row.get('Name')
                brand.head.mark_deleted = row.get('Mark_deleted', 0)
                brand.head.external_id = row.get('External_ID', None)
                brand.head.external_source_id = source_id

            await brand.save(user_id=user_id)
            result.append(brand)

        return result

