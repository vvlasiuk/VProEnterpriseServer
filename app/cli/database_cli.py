import click
import asyncio
import sys
import os
from pathlib import Path

# Додаємо шлях до проєкту
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.db.migration_service import MigrationService
from app.db.schema_manager import SchemaManager
from app.db.database import db_manager
from app.services.database_service import DatabaseService
from app.db.schema_comparator import SchemaComparator        # Додати цей рядок
from app.db.alter_table_generator import AlterTableGenerator # Додати цей рядок
from app.core.config import settings
from app.services.seed_data_service import SeedDataService   # Імпортуємо SeedDataService
import logging

# Налаштування логування для CLI
logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

async def init_database():
    """Ініціалізувати підключення до БД для CLI"""
    try:
        await db_manager.create_pool()
        return True
    except Exception as e:
        click.echo(f"❌ Failed to connect to database: {e}")
        click.echo(f"Check connection settings:")
        click.echo(f"   Server: {settings.DB_SERVER}:{settings.DB_PORT}")
        click.echo(f"   Database: {settings.DB_DATABASE}")
        return False

async def cleanup_database():
    """Закрити підключення до БД"""
    await db_manager.close_pool()

@click.group()
def db():
    """Database management commands"""
    pass

@db.command()
@click.option('--dry-run', is_flag=True, help='Show what would be changed without executing')
@click.option('--update-existing', is_flag=True, help='Update existing tables')
def migrate(dry_run, update_existing):
    """Застосувати міграції"""
    
    async def run_migration():
        if not await init_database():
            return False
            
        try:
            migration_service = MigrationService()
            
            if update_existing:
                # Оновити існуючі таблиці
                results = await migration_service.update_existing_tables(dry_run=dry_run)
                
                if dry_run:
                    click.echo("🔍 Planned changes:")
                    for change in results["changes_planned"]:
                        click.echo(f"\n📋 Table: {change['table']}")
                        for cmd in change['commands']:
                            click.echo(f"   • {cmd}")
                else:
                    if results["updated_tables"]:
                        click.echo("✅ Updated tables:")
                        for table in results["updated_tables"]:
                            click.echo(f"   • {table}")
            else:
                # Створити нові таблиці (існуючий код)
                results = await migration_service.create_all_tables()
                # ... існуючий код виводу ...
            
            if results["errors"]:
                click.echo("❌ Errors:")
                for error in results["errors"]:
                    click.echo(f"   • {error}")
                return False
            
            return True
            
        except Exception as e:
            click.echo(f"❌ Migration failed: {e}")
            return False
        finally:
            await cleanup_database()
    
    success = asyncio.run(run_migration())
    sys.exit(0 if success else 1)

@db.command()
def status():
    """Показати стан бази даних"""
    click.echo("📊 Checking database status...")
    
    async def check_status():
        # Ініціалізуємо БД
        if not await init_database():
            return False
            
        try:
            migration_service = MigrationService()
            info = await migration_service.get_database_info()
            
            click.echo(f"📋 Database: {settings.DB_DATABASE}")
            click.echo(f"🔗 Server: {settings.DB_SERVER}:{settings.DB_PORT}")
            click.echo()
            
            click.echo(f"📁 Existing tables in DB: {len(info['existing_tables'])}")
            for table in info['existing_tables']:
                click.echo(f"   • {table}")
            
            click.echo()
            click.echo(f"📋 Tables in schema: {len(info['schema_tables'])}")
            for table in info['schema_tables']:
                click.echo(f"   • {table}")
            
            if info['missing_tables']:
                click.echo()
                click.echo(f"⚠️  Missing tables: {len(info['missing_tables'])}")
                for table in info['missing_tables']:
                    click.echo(f"   • {table}")
            
            if info['validation_errors']:
                click.echo()
                click.echo("❌ Validation errors:")
                for error in info['validation_errors']:
                    click.echo(f"   • {error}")
            
            if not info['missing_tables'] and not info['validation_errors']:
                click.echo()
                click.echo("✅ Database is up to date!")
                
            return True
            
        except Exception as e:
            click.echo(f"❌ Failed to check status: {e}")
            return False
        finally:
            await cleanup_database()
    
    success = asyncio.run(check_status())
    sys.exit(0 if success else 1)

@db.command()
def schema():
    """Показати схему таблиць"""
    click.echo("📋 Loading database schema...")
    
    try:
        schema_manager = SchemaManager()
        schema_manager.load_all_schemas()
        
        # Parent tables
        if schema_manager.parent_tables:
            click.echo("👨‍👦 Parent tables:")
            for name, definition in schema_manager.parent_tables.items():
                columns_count = len(definition)
                click.echo(f"   • {name} ({columns_count} columns)")
        
        # Core tables
        core_tables = schema_manager.core_schema.get('tables', {})
        if core_tables:
            click.echo()
            click.echo("🏢 Core tables:")
            for name, definition in core_tables.items():
                parent = definition.get('parent', 'no parent')
                columns_count = len(definition.get('columns', {}))
                click.echo(f"   • {name} (parent: {parent}, {columns_count} columns)")
        
        # Plugin tables
        if schema_manager.plugin_schemas:
            click.echo()
            click.echo("🔌 Plugin tables:")
            for plugin_name, plugin_schema in schema_manager.plugin_schemas.items():
                plugin_tables = plugin_schema.get('tables', {})
                click.echo(f"   Plugin: {plugin_name}")
                for name, definition in plugin_tables.items():
                    parent = definition.get('parent', 'no parent')
                    columns_count = len(definition.get('columns', {}))
                    click.echo(f"     • {name} (parent: {parent}, {columns_count} columns)")
        
        # Creation order
        creation_order = schema_manager.get_table_creation_order()
        click.echo()
        click.echo("📋 Table creation order:")
        for i, table in enumerate(creation_order, 1):
            click.echo(f"   {i}. {table}")
        
    except Exception as e:
        click.echo(f"❌ Failed to load schema: {e}")
        sys.exit(1)

@db.command()
@click.argument('table_name')
def show_table(table_name):
    """Показати деталі конкретної таблиці"""
    try:
        schema_manager = SchemaManager()
        schema_manager.load_all_schemas()
        resolved_tables = schema_manager.get_all_tables()
        
        if table_name not in resolved_tables:
            click.echo(f"❌ Table '{table_name}' not found in schema")
            available = list(resolved_tables.keys())
            if available:
                click.echo("Available tables:")
                for table in sorted(available):
                    click.echo(f"   • {table}")
            sys.exit(1)
        
        table_def = resolved_tables[table_name]
        
        click.echo(f"📋 Table: {table_name}")
        click.echo("=" * (len(table_name) + 8))
        
        # Columns
        columns = table_def.get('columns', {})
        click.echo(f"📁 Columns ({len(columns)}):")
        for col_name, col_def in columns.items():
            col_type = col_def.get('type', 'UNKNOWN')
            nullable = "NULL" if col_def.get('nullable', True) else "NOT NULL"
            pk = " [PK]" if col_def.get('primary_key') else ""
            fk = f" -> {col_def['foreign_key']}" if col_def.get('foreign_key') else ""
            unique = " [UNIQUE]" if col_def.get('unique') else ""
            
            click.echo(f"   • {col_name}: {col_type} {nullable}{pk}{unique}{fk}")
        
        # Indexes
        indexes = table_def.get('indexes', [])
        if indexes:
            click.echo(f"🔍 Indexes ({len(indexes)}):")
            for index in indexes:
                unique = "[UNIQUE] " if index.get('unique') else ""
                columns = ', '.join(index['columns'])
                name = index.get('name', 'auto-generated')
                click.echo(f"   • {unique}{name} ({columns})")
        
        # SQL Preview
        sql = schema_manager.generate_create_table_sql(table_name, table_def)
        click.echo()
        click.echo("💾 SQL Preview:")
        click.echo("─" * 50)
        click.echo(sql)
        
    except Exception as e:
        click.echo(f"❌ Failed to show table: {e}")
        sys.exit(1)

@db.command()
@click.option('--force', is_flag=True, help='Skip confirmation prompt')
def drop_extra_tables(force):
    """Видалити таблиці, яких немає в схемі"""
    
    async def run_drop():
        if not await init_database():
            return False
            
        try:
            migration_service = MigrationService()
            info = await migration_service.get_database_info()
            
            # Знаходимо зайві таблиці (регістронезалежне порівняння)
            existing_tables_lower = set(t.lower() for t in info['existing_tables'])
            schema_tables_lower = set(t.lower() for t in info['schema_tables'])
            extra_tables_lower = existing_tables_lower - schema_tables_lower
            
            # Знаходимо оригінальні назви для видалення
            extra_tables = [
                t for t in info['existing_tables'] 
                if t.lower() in extra_tables_lower
            ]
            
            if not extra_tables:
                click.echo("✅ No extra tables found!")
                return True
            
            click.echo(f"⚠️  Found {len(extra_tables)} extra tables:")
            for table in sorted(extra_tables):
                click.echo(f"   • {table}")
            
            # Підтвердження
            if not force:
                click.echo()
                click.echo("⚠️  WARNING: This will permanently delete these tables and ALL their data!")
                confirm = click.confirm("Are you sure you want to continue?")
                if not confirm:
                    click.echo("❌ Operation cancelled")
                    return True
            
            # Видаляємо таблиці
            dropped_tables = []
            errors = []
            
            for table in extra_tables:
                try:
                    drop_sql = f"DROP TABLE {table}"
                    await DatabaseService.execute_non_query(drop_sql)
                    dropped_tables.append(table)
                    click.echo(f"🗑️  Dropped table: {table}")
                except Exception as e:
                    error_msg = f"Failed to drop {table}: {str(e)}"
                    errors.append(error_msg)
                    click.echo(f"❌ {error_msg}")
            
            # Результат
            click.echo()
            if dropped_tables:
                click.echo(f"✅ Successfully dropped {len(dropped_tables)} tables")
            
            if errors:
                click.echo(f"❌ Failed to drop {len(errors)} tables")
                return False
            
            return True
            
        except Exception as e:
            click.echo(f"❌ Operation failed: {e}")
            return False
        finally:
            await cleanup_database()
    
    success = asyncio.run(run_drop())
    sys.exit(0 if success else 1)

@db.command()
@click.option('--force', is_flag=True, help='Skip confirmation prompt')
def clean_database(force):
    """Видалити ВСІ таблиці з бази даних"""
    
    async def run_clean():
        if not await init_database():
            return False
            
        try:
            # Отримуємо всі таблиці
            query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """
            tables_result = await DatabaseService.execute_query(query)
            all_tables = [row["TABLE_NAME"] for row in tables_result]
            
            if not all_tables:
                click.echo("✅ Database is already empty!")
                return True
            
            click.echo(f"⚠️  Found {len(all_tables)} tables in database:")
            for table in all_tables:
                click.echo(f"   • {table}")
            
            # Підтвердження
            if not force:
                click.echo()
                click.echo("🚨 DANGER: This will delete ALL tables and ALL data!")
                click.echo("This action cannot be undone!")
                confirm = click.confirm("Type 'yes' to confirm", default=False)
                if not confirm:
                    click.echo("❌ Operation cancelled")
                    return True
            
            # Видаляємо всі таблиці
            dropped_count = 0
            errors = []
            
            for table in all_tables:
                try:
                    # Спочатку видаляємо foreign key constraints
                    drop_fk_sql = f"""
                    DECLARE @sql NVARCHAR(MAX) = ''
                    SELECT @sql = @sql + 'ALTER TABLE {table} DROP CONSTRAINT ' + name + ';'
                    FROM sys.foreign_keys WHERE parent_object_id = OBJECT_ID('{table}')
                    EXEC sp_executesql @sql
                    """
                    await DatabaseService.execute_non_query(drop_fk_sql)
                    
                    # Тепер видаляємо таблицю
                    drop_sql = f"DROP TABLE {table}"
                    await DatabaseService.execute_non_query(drop_sql)
                    dropped_count += 1
                    click.echo(f"🗑️  Dropped: {table}")
                    
                except Exception as e:
                    error_msg = f"Failed to drop {table}: {str(e)}"
                    errors.append(error_msg)
                    click.echo(f"❌ {error_msg}")
            
            # Результат
            click.echo()
            click.echo(f"✅ Successfully dropped {dropped_count} tables")
            
            if errors:
                click.echo(f"❌ Failed to drop {len(errors)} tables")
                return False
            
            return True
            
        except Exception as e:
            click.echo(f"❌ Operation failed: {e}")
            return False
        finally:
            await cleanup_database()
    
    success = asyncio.run(run_clean())
    sys.exit(0 if success else 1)

@db.command()
@click.argument('table_name')
def diff_table(table_name):
    """Показати різниці між БД та схемою для таблиці"""
    
    async def show_diff():
        if not await init_database():
            return False
            
        try:
            schema_manager = SchemaManager()
            schema_manager.load_all_schemas()
            resolved_tables = schema_manager.get_all_tables()
            
            if table_name not in resolved_tables:
                click.echo(f"❌ Table '{table_name}' not found in schema")
                return False
            
            comparator = SchemaComparator()
            generator = AlterTableGenerator()
            
            # Структури
            db_structure = await comparator.get_table_structure(table_name)
            yaml_structure = resolved_tables[table_name]
            
            # Різниці
            differences = comparator.compare_table_structures(db_structure, yaml_structure)
            
            if not any(differences.values()):
                click.echo(f"✅ Table '{table_name}' is up to date")
                return True
            
            click.echo(f"📋 Differences for table '{table_name}':")
            
            if differences['add_columns']:
                click.echo("\n➕ Columns to add:")
                for col_name, col_def in differences['add_columns']:
                    click.echo(f"   • {col_name}: {col_def.get('type', 'NVARCHAR(255)')}")
            
            if differences['modify_columns']:
                click.echo("\n🔄 Columns to modify:")
                for col_name, db_def, yaml_def in differences['modify_columns']:
                    click.echo(f"   • {col_name}: {db_def['type']} → {yaml_def.get('type', 'NVARCHAR(255)')}")
            
            if differences['drop_columns']:
                click.echo("\n➖ Columns to drop:")
                for col_name in differences['drop_columns']:
                    click.echo(f"   • {col_name}")
            
            # SQL команди
            alter_commands = generator.generate_alter_commands(table_name, differences)
            click.echo("\n💾 SQL Commands:")
            for cmd in alter_commands:
                click.echo(f"   {cmd}")
            
            return True
            
        except Exception as e:
            click.echo(f"❌ Failed to compare: {e}")
            return False
        finally:
            await cleanup_database()
    
    success = asyncio.run(show_diff())
    sys.exit(0 if success else 1)

@db.command()
def seed():
    """Заповнити БД початковими даними"""
    
    async def run_seeding():
        if not await init_database():
            return False
        
        try:
            seed_service = SeedDataService()
            results = await seed_service.seed_all_data()
            
            if results["seeded_tables"]:
                click.echo("✅ Seeded tables:")
                for table in results["seeded_tables"]:
                    click.echo(f"   • {table}")
            
            if results["skipped_tables"]:
                click.echo("⏭️  Skipped tables:")
                for table in results["skipped_tables"]:
                    click.echo(f"   • {table}")
            
            if results["errors"]:
                click.echo("❌ Errors:")
                for error in results["errors"]:
                    click.echo(f"   • {error}")
                return False
            
            return True
            
        except Exception as e:
            click.echo(f"❌ Seeding failed: {e}")
            return False
        finally:
            await cleanup_database()
    
    success = asyncio.run(run_seeding())
    sys.exit(0 if success else 1)

@db.command()
def migrate_and_seed():
    """Міграція + заповнення даними"""
    
    async def run_full_setup():
        # Спочатку міграція
        migration_service = MigrationService()
        migration_results = await migration_service.create_all_tables()
        
        # Потім seeding
        seed_service = SeedDataService()
        seed_results = await seed_service.seed_all_data()
        
        return migration_results, seed_results
    
    # Виконати повний setup
    success = asyncio.run(run_full_setup())
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    db()