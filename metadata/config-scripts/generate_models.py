import yaml
from pathlib import Path
from typing import Dict, Any

def read_catalog_schema(schemas_path: str = "") -> Dict[str, Any]:

    file_path = Path(schemas_path)
    
    filename = file_path.stem
    parts = filename.split('_')
    class_name = parts[0].capitalize() + '_' + ''.join(word.capitalize() for word in parts[1:])

    dto_file_name = class_name.lower() + '_dto'

    print(f"DTO Name: {dto_file_name}, Class Name: {class_name}")




# Приклад використання:
if __name__ == "__main__":
    schema = read_catalog_schema("app/db/schemas/catalogs/cat_products_categories.yaml")
    print(schema)


#    # Створюємо структуру директорій
#     relative_path = Path(schema_path).relative_to("app/db/schemas")
#     output_subdir = Path(output_dir) / relative_path.parent
#     output_subdir.mkdir(parents=True, exist_ok=True)
    
#     # Генеруємо DTO код
#     dto_code = generate_dto_code(schema, dto_name, filename)
    
#     # Записуємо у файл
#     output_file = output_subdir / f"{filename}_dto.py"
#     with open(output_file, 'w', encoding='utf-8') as f:
#         f.write(dto_code)
    
#     print(f"✅ Створено: {output_file}")


# def generate_dto_code(schema: Dict[str, Any], dto_name: str, filename: str) -> str:
#     """Генерує Python код для DTO"""
    
#     # Отримуємо колонки зі схеми
#     columns = schema.get('tables', {}).get(filename, {}).get('columns', {})
    
#     # Генеруємо поля
#     fields = []
#     for col_name, col_def in columns.items():
#         col_type = col_def.get('type', 'str')
#         nullable = col_def.get('nullable', True)
        
#         # Перетворюємо SQL типи на Python
#         python_type = sql_to_python_type(col_type)
#         optional = "Optional" if nullable else ""
#         type_hint = f"{optional}[{python_type}]" if nullable else python_type
        
#         fields.append(f"    {col_name}: {type_hint} = None")
    
#     fields_str = "\n".join(fields)
    
#     code = f'''from dataclasses import dataclass
# from datetime import datetime
# from typing import Optional

# @dataclass
# class {dto_name}:
# {fields_str}
# '''
    
#     return code


# def sql_to_python_type(sql_type: str) -> str:
#     """Перетворює SQL типи на Python типи"""
#     sql_type_upper = sql_type.upper()
    
#     type_mapping = {
#         'INT': 'int',
#         'BIGINT': 'int',
#         'NVARCHAR': 'str',
#         'VARCHAR': 'str',
#         'BIT': 'bool',
#         'DATETIME': 'datetime',
#         'DECIMAL': 'float',
#         'FLOAT': 'float',
#         'UNIQUEIDENTIFIER': 'str',
#     }
    
#     for sql_type_key, python_type in type_mapping.items():
#         if sql_type_key in sql_type_upper:
#             return python_type
    
#     return 'str'  # За замовчуванням


# # Приклад використання:
# if __name__ == "__main__":
#     schema_dir = Path("app/db/schemas")
    
#     # Знаходимо всі YAML файли
#     for yaml_file in schema_dir.rglob("*.yaml"):
#         generate_dto_from_schema(str(yaml_file))    