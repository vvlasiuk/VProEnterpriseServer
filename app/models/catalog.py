class Catalog:
    table_name = None  # Встановлюється у дочірньому класі

    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def get_all(self):
        sql = f"SELECT * FROM {self.table_name}"
        async with self.db_manager.get_transaction() as cursor:
            await cursor.execute(sql)
            rows = await cursor.fetchall()
            return [self.__class__.from_row(dict(row)) for row in rows]

    async def get_by_id(self, item_id):
        sql = f"SELECT * FROM {self.table_name} WHERE _id = ?"
        async with self.db_manager.get_transaction() as cursor:
            await cursor.execute(sql, (item_id,))
            row = await cursor.fetchone()
            return self.__class__.from_row(dict(row)) if row else None

    async def create(self, data: dict):
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        sql = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
        async with self.db_manager.get_transaction() as cursor:
            await cursor.execute(sql, tuple(data.values()))
            return True

    async def update(self, item_id, data: dict):
        set_clause = ', '.join([f"{k} = ?" for k in data.keys()])
        sql = f"UPDATE {self.table_name} SET {set_clause} WHERE _id = ?"
        async with self.db_manager.get_transaction() as cursor:
            await cursor.execute(sql, tuple(data.values()) + (item_id,))
            return True

    async def delete(self, item_id):
        sql = f"DELETE FROM {self.table_name} WHERE _id = ?"
        async with self.db_manager.get_transaction() as cursor:
            await cursor.execute(sql, (item_id,))
            return True