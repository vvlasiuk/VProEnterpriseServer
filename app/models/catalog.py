from app.db.database import db_manager

class Catalog:
    _db_head = None  # Встановлюється у дочірньому класі
    _db_tables = None

    # __slots__ = ('__id', '__version', '_created_at', '_created_by', '_name', '_mark_deleted')
    def __init__(self, user_id=None):
        pass
    #     self._created_at = None
    #     self._created_by = None
    #     self._name = None
    #     self._mark_deleted = None

    # @property
    # def id(self):
    #     return self.__id
    
    # @id.setter
    # def id(self, value):
    #     raise AttributeError("id is read-only")
    
    # @property
    # def version(self):
    #     return self.__version

    # @version.setter
    # def version(self, value):
    #     raise AttributeError("version is read-only")


    async def save(self):
        # Взяти тільки потрібні поля з self.head
        data = {col: getattr(self.head, col) for col in self._db_head["columns"]}
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        sql = f"INSERT INTO {self._db_head['table_name']} ({columns}) OUTPUT INSERTED._id VALUES ({placeholders})"
        async with db_manager.get_transaction() as cursor:
            await cursor.execute(sql, tuple(data.values()))
            inserted_id_row = await cursor.fetchone()
            inserted_id = inserted_id_row[0] if inserted_id_row else None
            return inserted_id

    @classmethod
    async def get_by_id(cls, item_id):
        sql = f"SELECT * FROM {cls._db_head['table_name']} WHERE _id = ?"
        async with db_manager.get_transaction() as cursor:
            await cursor.execute(sql, (item_id,))
            row = await cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                row_dict = dict(zip(columns, row))
                return row_dict
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