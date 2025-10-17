from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class CatalogDTO:
    _id: Optional[int] = None
    name: Optional[str] = None
    _version: Optional[bytes] = None
    _created_at: Optional[datetime] = None
    _created_by: Optional[int] = None
    mark_deleted: Optional[bool] = None
    
@dataclass
class CatalogProductBrandDTO(CatalogDTO):
    pass

@dataclass
class CatalogProductCategoryDTO(CatalogDTO):
    pass

@dataclass
class CatalogProductDTO(CatalogDTO):
    pass

@dataclass
class CatalogExternalDataMappingDTO:
    _id: Optional[int] = None
    _created_at: Optional[datetime] = None
    source_id: Optional[int] = None
    external_id: Optional[str] = None
    internal_id: Optional[int] = None
    internal_type_id: Optional[int] = None
