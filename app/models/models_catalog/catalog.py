from fastapi.params import Depends
from app.db.database import db_manager
from app.core.security import get_current_user

class Catalog:
    _db_head = {"table_name": None, "table_typeid": None, "columns": ["name", "mark_deleted", "_created_by"]}
    _db_tables = None

    def __init__(self):
        pass
    
    @classmethod
    async def init_head_typeid(cls):
        cls._db_head['table_typeid'] = await Catalog.get_head_typeid(cls._db_head["table_name"])

    async def save_external_id(self):
        if not self.head:
            return None

        if not self.head.external_id or not self.head.external_source_id or not self.head._id:
            return None

        if self._db_head['table_typeid'] is None:
            await self.__class__.init_head_typeid()

        select_sql = """
            SELECT _id FROM cat_external_data
            WHERE external_id = ? AND external_source_id = ? AND internal_id = ? AND internal_typeid = ?
        """
        params = (
            self.head.external_id,
            self.head.external_source_id,
            self.head._id,
            self._db_head['table_typeid']
        )
        async with db_manager.get_transaction() as cursor:
            await cursor.execute(select_sql, params)
            row = await cursor.fetchone()
            if row:
                return row[0]

        insert_sql = """
            INSERT INTO cat_external_data (external_id, external_source_id, internal_id, internal_typeid)
            OUTPUT INSERTED._id
            VALUES (?, ?, ?, ?)
        """
        async with db_manager.get_transaction() as cursor:
            await cursor.execute(insert_sql, params)
            inserted_id_row = await cursor.fetchone()
            inserted_id = inserted_id_row[0] if inserted_id_row else None
            return inserted_id

    async def save(self, user_id: int = None):
        data = {col: getattr(self.head, col) for col in self._db_head["columns"]}
        
        if self.head._id is None:

            data['_created_by'] = user_id 

            columns = ', '.join(data.keys())
            placeholders = ', '.join(['?'] * len(data))
            sql = f"INSERT INTO {self._db_head['table_name']} ({columns}) OUTPUT INSERTED._id VALUES ({placeholders})"
            async with db_manager.get_transaction() as cursor:
                await cursor.execute(sql, tuple(data.values()))
                inserted_id_row = await cursor.fetchone()
                inserted_id = inserted_id_row[0] if inserted_id_row else None
                self.head._id = inserted_id
        else:
            set_clause = ', '.join([f"{col} = ?" for col in data.keys()])
            sql = f"UPDATE {self._db_head['table_name']} SET {set_clause} WHERE _id = ?"
            async with db_manager.get_transaction() as cursor:
                await cursor.execute(sql, tuple(data.values()) + (self.head._id,))
                inserted_id = self.head._id

        await self.save_external_id()

        return inserted_id
    
    @classmethod
    def new(cls):
        obj = cls()
        obj.head = cls._DTO() 
        # obj.table_one = CatalogProductBrandDTO()
        # obj.table_two = CatalogProductBrandDTO()
        return obj

    @classmethod
    async def get_by_id(cls, item_id):
        sql = f"SELECT * FROM {cls._db_head['table_name']} WHERE _id = ?"
        async with db_manager.get_transaction() as cursor:
            await cursor.execute(sql, (item_id,))
            row = await cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                row_dict = dict(zip(columns, row))
                
                obj = cls()
                obj.head = cls._DTO(**row_dict)
                return obj
                # return row_dict
            return None
        
    @classmethod
    async def get_by_external_id(cls, external_id, source_id):
        if not external_id or not source_id:
            return None
        if cls._db_head['table_typeid'] is None:
           await cls.init_head_typeid()

        sql = (
            f"SELECT TOP 1 tab.* "
            f"FROM cat_external_data "
            f"INNER JOIN {cls._db_head['table_name']} as tab "
            f"ON cat_external_data.internal_id = tab._id "
            f"WHERE cat_external_data.external_id = ? "
            f"AND cat_external_data.external_source_id = ? "
            f"AND cat_external_data.internal_typeid = ?"
        )

        async with db_manager.get_transaction() as cursor:
            await cursor.execute(sql, (external_id, source_id, cls._db_head['table_typeid']))
            row = await cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                row_dict = dict(zip(columns, row))

                obj = cls()
                obj.head = cls._DTO(**row_dict)
                return obj
                # return row_dict
            return None
                
    @classmethod
    async def get_head_typeid(cls, table_name):
        sql = f"SELECT id FROM sys_data_types WHERE table_name = ?"
        async with db_manager.get_transaction() as cursor:
            await cursor.execute(sql, (table_name,))
            row = await cursor.fetchone()
            if row:
                return row.id
            return None

# async def get_all(self):
    #     sql = f"SELECT * FROM {self._db_head['table_name']}"
    #     async with db_manager.get_transaction() as cursor:
    #         await cursor.execute(sql)
    #         rows = await cursor.fetchall()
    #         # return [self.__class__.from_row(dict(row)) for row in rows]
    #         # return [self.__class__.from_row(dict(zip(row.keys(), row))) for row in rows]

    # async def create(self, data: dict):
    #     columns = ', '.join(data.keys())
    #     placeholders = ', '.join(['?'] * len(data))
    #     sql = f"INSERT INTO {self.db_head_table_name} ({columns}) VALUES ({placeholders})"
    #     async with db_manager.get_transaction() as cursor:
    #         await cursor.execute(sql, tuple(data.values()))
    #         return True

    # async def update(self, item_id, data: dict):
    #     set_clause = ', '.join([f"{k} = ?" for k in data.keys()])
    #     sql = f"UPDATE {self.db_head_table_name} SET {set_clause} WHERE _id = ?"
    #     async with db_manager.get_transaction() as cursor:
    #         await cursor.execute(sql, tuple(data.values()) + (item_id,))
    #         return True

    # async def delete(self, item_id):
    #     sql = f"DELETE FROM {self.db_head.table_name} WHERE _id = ?"
    #     async with db_manager.get_transaction() as cursor:
    #         await cursor.execute(sql, (item_id,))
    #         return True