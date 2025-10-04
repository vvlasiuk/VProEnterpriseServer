from typing import List, Dict, Tuple, Any

class AlterTableGenerator:
    """Генерація ALTER TABLE команд"""
    
    def generate_alter_commands(self, table_name: str, differences: Dict[str, List]) -> List[str]:
        """Згенерувати команди ALTER TABLE"""
        commands = []
        
        # Додати колонки
        for col_name, col_def in differences.get('add_columns', []):
            cmd = self._generate_add_column(table_name, col_name, col_def)
            commands.append(cmd)
        
        # Змінити колонки
        for col_name, db_def, yaml_def in differences.get('modify_columns', []):
            cmd = self._generate_alter_column(table_name, col_name, yaml_def)
            commands.append(cmd)
        
        # Видалити колонки (обережно!)
        for col_name in differences.get('drop_columns', []):
            drop_commands = self._generate_drop_column(table_name, col_name)
            commands.extend(drop_commands)  # ← extend замість append
        
        return commands
    
    def _generate_add_column(self, table_name: str, col_name: str, col_def: Dict) -> str:
        """Згенерувати ADD COLUMN команду"""
        col_type = col_def.get('type', 'NVARCHAR(255)')
        
        # Для NOT NULL колонок завжди додаємо DEFAULT
        if not col_def.get('nullable', True):
            if 'default' not in col_def:
                # Автоматично додаємо DEFAULT для NOT NULL колонок
                if 'NVARCHAR' in col_type or 'NTEXT' in col_type:
                    col_def['default'] = "''"
                elif 'INT' in col_type or 'BIGINT' in col_type:
                    col_def['default'] = "0"
                elif 'BIT' in col_type:
                    col_def['default'] = "0"
                elif 'DATETIME' in col_type:
                    col_def['default'] = "GETDATE()"
        
        nullable = "NULL" if col_def.get('nullable', True) else "NOT NULL"
        cmd = f"ALTER TABLE {table_name} ADD {col_name} {col_type} {nullable}"
        
        if 'default' in col_def:
            cmd += f" DEFAULT {col_def['default']}"
        
        return cmd
    
    def _generate_alter_column(self, table_name: str, col_name: str, col_def: Dict) -> str:
        """Згенерувати ALTER COLUMN команду"""
        col_type = col_def.get('type', 'NVARCHAR(255)')
        nullable = "NULL" if col_def.get('nullable', True) else "NOT NULL"
        
        return f"ALTER TABLE {table_name} ALTER COLUMN {col_name} {col_type} {nullable}"
    
    def _generate_drop_column(self, table_name: str, col_name: str) -> List[str]:
        """Безпечне видалення колонки з constraints"""
        commands = []
        
        # Спочатку видалити DEFAULT constraint
        commands.append(f"""
        DECLARE @constraint_name NVARCHAR(256)
        SELECT @constraint_name = name 
        FROM sys.default_constraints 
        WHERE parent_object_id = OBJECT_ID('{table_name}') 
        AND col_name(parent_object_id, parent_column_id) = '{col_name}'
        
        IF @constraint_name IS NOT NULL
            EXEC('ALTER TABLE {table_name} DROP CONSTRAINT ' + @constraint_name)
        """)
        
        # Потім видалити колонку
        commands.append(f"ALTER TABLE {table_name} DROP COLUMN {col_name}")
        
        return commands