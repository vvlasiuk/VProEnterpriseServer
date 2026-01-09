import sys
from pathlib import Path

# Додати кореневу директорію проекту до sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.db.schema_manager import SchemaManager


def main():
    schema_manager = SchemaManager()
    schema_files = schema_manager.load_all_schemas_yaml()
    # for file in schema_files:
    #     print(file)


if __name__ == "__main__":
    main()