# app/services/excel_import_service.py
from typing import Dict, List, Any, Optional, Union
import pandas as pd
from pathlib import Path
import logging
from io import BytesIO
import aioodbc

logger = logging.getLogger(__name__)

class ExcelImportService:
    """Service for importing data from Excel files"""
    
    def __init__(self):
        self.supported_extensions = ['.xlsx', '.xls', '.csv']
        self.max_file_size = 50 * 1024 * 1024  # 50MB
    
    def validate_file(self, file_content: bytes, filename: str) -> Dict[str, Any]:
        """Validate Excel file before processing"""
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'file_info': {}
        }
        
        try:
            # Check file size
            if len(file_content) > self.max_file_size:
                result['valid'] = False
                result['errors'].append(f"File too large: {len(file_content)} bytes (max: {self.max_file_size})")
                return result
            
            # Check file extension
            file_ext = Path(filename).suffix.lower()
            if file_ext not in self.supported_extensions:
                result['valid'] = False
                result['errors'].append(f"Unsupported file type: {file_ext}")
                return result
            
            # Try to read file structure
            file_buffer = BytesIO(file_content)
            
            if file_ext == '.csv':
                df = pd.read_csv(file_buffer, nrows=0)  # Just headers
            else:
                # Get Excel info
                excel_file = pd.ExcelFile(file_buffer, engine='openpyxl' if file_ext == '.xlsx' else 'xlrd')
                result['file_info']['sheets'] = excel_file.sheet_names
                
                # Read first sheet headers
                df = pd.read_excel(excel_file, sheet_name=0, nrows=0)
            
            result['file_info']['columns'] = list(df.columns)
            result['file_info']['column_count'] = len(df.columns)
            
            logger.info(f"File validation passed: {filename}")
            
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"File reading error: {str(e)}")
            logger.error(f"File validation failed for {filename}: {e}")
        
        return result
    
    def read_excel_file(self, file_content: bytes, filename: str, 
                       sheet_name: Union[str, int] = 0, 
                       skip_rows: int = 0,
                       max_rows: Optional[int] = None) -> Dict[str, Any]:
        """Read data from Excel file"""
        
        result = {
            'success': True,
            'data': [],
            'columns': [],
            'row_count': 0,
            'errors': []
        }
        
        try:
            file_buffer = BytesIO(file_content)
            file_ext = Path(filename).suffix.lower()
            
            # Read data based on file type
            if file_ext == '.csv':
                df = pd.read_csv(
                    file_buffer, 
                    skiprows=skip_rows,
                    nrows=max_rows,
                    dtype=str,  # Read everything as strings initially
                    na_filter=False  # Don't convert to NaN
                )
            else:
                df = pd.read_excel(
                    file_buffer,
                    sheet_name=sheet_name,
                    skiprows=skip_rows,
                    nrows=max_rows,
                    dtype=str,
                    na_filter=False,
                    engine='openpyxl' if file_ext == '.xlsx' else 'xlrd'
                )
                if isinstance(df, dict):
                    # Вибрати першу вкладку, якщо повернувся dict
                    first_sheet = list(df.keys())[0]
                    df = df[first_sheet]
            
            # Clean up data
            df = self._clean_dataframe(df)
            
            # Convert to result format
            result['columns'] = list(df.columns)
            result['data'] = df.to_dict('records')
            result['row_count'] = len(df)
            
            logger.info(f"Successfully read {result['row_count']} rows from {filename}")
            
        except Exception as e:
            result['success'] = False
            result['errors'].append(f"Data reading error: {str(e)}")
            logger.error(f"Failed to read data from {filename}: {e}")
        
        return result
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare DataFrame"""
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Clean column names
        df.columns = [self._clean_column_name(col) for col in df.columns]
        
        # Remove duplicate column names
        df.columns = self._handle_duplicate_columns(df.columns)
        
        # Clean cell values
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
                # Replace empty strings with None
                df[col] = df[col].replace('', None)
        
        return df
    
    def _clean_column_name(self, column_name: str) -> str:
        """Clean column name for processing"""
        if pd.isna(column_name):
            return "unnamed_column"
        
        # Convert to string and clean
        clean_name = str(column_name).strip()
        
        # Replace problematic characters
        clean_name = clean_name.replace(' ', '_')
        clean_name = clean_name.replace('-', '_')
        clean_name = clean_name.replace('.', '_')
        
        # Remove non-alphanumeric characters except underscore
        clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
        
        # Ensure it starts with letter or underscore
        if clean_name and clean_name[0].isdigit():
            clean_name = f"col_{clean_name}"
        
        return clean_name if clean_name else "unnamed_column"
    
    def _handle_duplicate_columns(self, columns: List[str]) -> List[str]:
        """Handle duplicate column names by adding suffixes"""
        seen = {}
        result = []
        
        for col in columns:
            if col in seen:
                seen[col] += 1
                result.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                result.append(col)
        
        return result
    
    def get_column_mapping_suggestions(self, excel_columns: List[str], 
                                     target_table_columns: List[str]) -> Dict[str, Optional[str]]:
        """Suggest mapping between Excel columns and target table columns"""
        
        mapping = {}
        
        for excel_col in excel_columns:
            best_match = None
            excel_col_clean = excel_col.lower().replace('_', '').replace(' ', '')
            
            for table_col in target_table_columns:
                table_col_clean = table_col.lower().replace('_', '').replace(' ', '')
                
                # Exact match
                if excel_col_clean == table_col_clean:
                    best_match = table_col
                    break
                
                # Partial match
                if excel_col_clean in table_col_clean or table_col_clean in excel_col_clean:
                    if best_match is None:
                        best_match = table_col
            
            mapping[excel_col] = best_match
        
        return mapping
    
    def validate_data(self, data: List[Dict], table_schema: Dict, column_mapping: Dict) -> Dict[str, Any]:
        """Validate data against table schema"""
        
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'valid_rows': 0,
            'invalid_rows': 0,
            'row_errors': {}
        }
        
        try:
            schema_columns = table_schema.get('columns', {})
            required_fields = [col for col, def_ in schema_columns.items() if not def_.get('nullable', True)]
            
            for row_idx, row in enumerate(data):
                row_errors = []
                
                # # Check required fields
                # for required_field in required_fields:
                #     mapped_excel_col = None
                #     for excel_col, table_col in column_mapping.items():
                #         if table_col == required_field:
                #             mapped_excel_col = excel_col
                #             break
                    
                #     if mapped_excel_col is None:
                #         row_errors.append(f"Required field '{required_field}' not mapped")
                #     elif not row.get(mapped_excel_col) or str(row.get(mapped_excel_col)).strip() == '':
                #         row_errors.append(f"Required field '{required_field}' is empty")
                
                # Validate data types
                for excel_col, table_col in column_mapping.items():
                    if table_col and table_col in schema_columns:
                        value = row.get(excel_col)
                        if value is not None and str(value).strip() != '':
                            validation_error = self._validate_field_type(value, schema_columns[table_col])
                            if validation_error:
                                row_errors.append(f"Field '{table_col}': {validation_error}")
                
                if row_errors:
                    result['invalid_rows'] += 1
                    result['row_errors'][row_idx + 1] = row_errors  # 1-based row numbers
                    result['errors'].extend([f"Row {row_idx + 1}: {error}" for error in row_errors])
                else:
                    result['valid_rows'] += 1
            
            if result['invalid_rows'] > 0:
                result['valid'] = False
                
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"Validation error: {str(e)}")
            logger.error(f"Data validation failed: {e}")
        
        return result
    
    def _validate_field_type(self, value: Any, field_def: Dict) -> Optional[str]:
        """Validate single field against its definition"""
        
        field_type = field_def.get('type', '').upper()
        str_value = str(value).strip()
        
        try:
            if 'INT' in field_type or 'BIGINT' in field_type:
                int(str_value)
            elif 'DECIMAL' in field_type or 'FLOAT' in field_type:
                float(str_value)
            elif 'BIT' in field_type:
                if str_value.lower() not in ['0', '1', 'true', 'false', 'yes', 'no']:
                    return f"Invalid boolean value: {str_value}"
            elif 'DATE' in field_type:
                # Basic date validation - could be enhanced
                pd.to_datetime(str_value)
            elif 'NVARCHAR' in field_type:
                # Check length if specified
                if '(' in field_type:
                    max_length = int(field_type.split('(')[1].split(')')[0])
                    if len(str_value) > max_length:
                        return f"Value too long: {len(str_value)} > {max_length}"
                        
        except (ValueError, TypeError):
            return f"Invalid {field_type} value: {str_value}"
        
        return None
    
    def transform_data(self, data: List[Dict], column_mapping: Dict, transform_rules: Dict = None) -> List[Dict]:
        """Transform data according to column mapping and rules"""
        
        if transform_rules is None:
            transform_rules = {}
        
        transformed_data = []
        
        for row in data:
            transformed_row = {}
            
            for excel_col, table_col in column_mapping.items():
                if table_col and excel_col in row:
                    value = row[excel_col]
                    
                    # Apply transformation rules
                    if table_col in transform_rules:
                        value = self._apply_transform_rule(value, transform_rules[table_col])
                    
                    # Basic cleanup
                    if isinstance(value, str):
                        value = value.strip()
                        if value == '':
                            value = None
                    
                    transformed_row[table_col] = value
            
            transformed_data.append(transformed_row)
        
        return transformed_data
    
    def _apply_transform_rule(self, value: Any, rule: Dict) -> Any:
        """Apply single transformation rule to value"""
        
        if value is None:
            return value
        
        str_value = str(value)
        
        rule_type = rule.get('type', '').lower()
        
        if rule_type == 'uppercase':
            return str_value.upper()
        elif rule_type == 'lowercase':
            return str_value.lower()
        elif rule_type == 'trim':
            return str_value.strip()
        elif rule_type == 'replace':
            return str_value.replace(rule.get('from', ''), rule.get('to', ''))
        elif rule_type == 'default':
            return str_value if str_value.strip() else rule.get('value')
        
        return value
    
    def process_in_batches(self, data: List[Dict], batch_size: int = 1000):
        """Process data in batches for memory efficiency"""
        
        for i in range(0, len(data), batch_size):
            yield data[i:i + batch_size]
    
    def check_duplicates(self, data: List[Dict], unique_columns: List[str]) -> Dict[str, Any]:
        """Check for duplicates in data"""
        
        result = {
            'has_duplicates': False,
            'duplicate_groups': [],
            'duplicate_count': 0
        }
        
        if not unique_columns:
            return result
        
        # Group by unique columns
        seen_values = {}
        
        for row_idx, row in enumerate(data):
            # Create key from unique columns
            key_parts = []
            for col in unique_columns:
                value = row.get(col, '')
                key_parts.append(str(value).strip().lower())
            
            key = '|'.join(key_parts)
            
            if key in seen_values:
                seen_values[key].append(row_idx + 1)  # 1-based row numbers
            else:
                seen_values[key] = [row_idx + 1]
        
        # Find duplicates
        for key, row_indices in seen_values.items():
            if len(row_indices) > 1:
                result['has_duplicates'] = True
                result['duplicate_groups'].append({
                    'key': key,
                    'rows': row_indices,
                    'count': len(row_indices)
                })
                result['duplicate_count'] += len(row_indices) - 1  # Exclude first occurrence
        
        return result
    
    def generate_import_summary(self, processed_data: Dict) -> Dict[str, Any]:
        """Generate comprehensive import summary"""
        
        summary = {
            'file_info': processed_data.get('file_info', {}),
            'data_stats': {
                'total_rows': processed_data.get('row_count', 0),
                'valid_rows': processed_data.get('validation', {}).get('valid_rows', 0),
                'invalid_rows': processed_data.get('validation', {}).get('invalid_rows', 0),
                'duplicate_rows': processed_data.get('duplicates', {}).get('duplicate_count', 0)
            },
            'column_mapping': processed_data.get('column_mapping', {}),
            'validation_errors': processed_data.get('validation', {}).get('errors', []),
            'warnings': processed_data.get('validation', {}).get('warnings', []),
            'processing_time': processed_data.get('processing_time', 0),
            'recommendations': []
        }
        
        # Generate recommendations
        if summary['data_stats']['invalid_rows'] > 0:
            summary['recommendations'].append("Review validation errors before importing")
        
        if summary['data_stats']['duplicate_rows'] > 0:
            summary['recommendations'].append("Handle duplicate records")
        
        unmapped_columns = [col for col, mapping in summary['column_mapping'].items() if mapping is None]
        if unmapped_columns:
            summary['recommendations'].append(f"Consider mapping unused columns: {', '.join(unmapped_columns)}")
        
        return summary
    
    def generate_preview(self, data: List[Dict], column_mapping: Dict = None, max_rows: int = 10) -> Dict[str, Any]:
        """Generate preview of data for user review"""
        
        preview = {
            'sample_data': data[:max_rows],
            'total_rows': len(data),
            'columns': list(data[0].keys()) if data else [],
            'column_mapping': column_mapping or {},
            'data_types_detected': {},
            'potential_issues': []
        }
        
        if data:
            # Detect data types
            for col in preview['columns']:
                sample_values = [row.get(col) for row in data[:50] if row.get(col) is not None]
                preview['data_types_detected'][col] = self._detect_column_type(sample_values)
            
            # Find potential issues
            for col in preview['columns']:
                values = [str(row.get(col, '')).strip() for row in data[:100]]
                non_empty_values = [v for v in values if v]
                
                if len(non_empty_values) == 0:
                    preview['potential_issues'].append(f"Column '{col}' appears to be empty")
                elif len(non_empty_values) < len(values) * 0.5:
                    preview['potential_issues'].append(f"Column '{col}' has many empty values")
        
        return preview
    
    def _detect_column_type(self, sample_values: List[Any]) -> str:
        """Detect likely data type from sample values"""
        
        if not sample_values:
            return 'unknown'
        
        # Try to detect type from sample
        numeric_count = 0
        date_count = 0
        bool_count = 0
        
        for value in sample_values[:20]:  # Sample first 20 values
            str_value = str(value).strip()
            
            # Check if numeric
            try:
                float(str_value)
                numeric_count += 1
                continue
            except ValueError:
                pass
            
            # Check if boolean
            if str_value.lower() in ['true', 'false', '1', '0', 'yes', 'no']:
                bool_count += 1
                continue
            
            # Check if date
            try:
                pd.to_datetime(str_value)
                date_count += 1
                continue
            except:
                pass
        
        total = len(sample_values[:20])
        
        if numeric_count / total > 0.8:
            return 'numeric'
        elif date_count / total > 0.8:
            return 'date'
        elif bool_count / total > 0.8:
            return 'boolean'
        else:
            return 'text'
    
    # async def import_data_batch(self, data: List[Dict], table_name: str, batch_size: int = 100, db_manager=None):
    #     """
    #     Імпортує дані порціями у таблицю через DatabaseManager.
    #     Повертає: {'success': bool, 'imported': int, 'imported_records': List, 'errors': List}
    #     """
    #     result = {'success': True, 'imported': 0, 'imported_records': [], 'errors': []}
    #     if db_manager is None:
    #         result['success'] = False
    #         result['errors'].append('No db_manager provided')
    #         return result

    #     for batch in self.process_in_batches(data, batch_size):
    #         if not batch:
    #             continue
    #         columns = list(batch[0].keys())
    #         values = [tuple(row[col] for col in columns) for row in batch]
    #         placeholders = ', '.join(['?'] * len(columns))
    #         sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    #         try:
    #             async with db_manager.get_transaction() as cursor:
    #                 await cursor.executemany(sql, values)
    #             result['imported'] += len(batch)
    #             result['imported_records'].extend(batch)
    #         except Exception as e:
    #             result['errors'].append(str(e))
    #     return result