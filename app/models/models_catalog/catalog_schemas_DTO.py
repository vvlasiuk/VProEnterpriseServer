from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class CatalogDTO:
    _id: Optional[int] = None
    _typeid: Optional[int] = None
    name: Optional[str] = None
    _version: Optional[bytes] = None
    _created_at: Optional[datetime] = None
    _created_by: Optional[int] = None
    mark_deleted: Optional[bool] = False
    external_id: Optional[str] = None
    external_source_id: Optional[int] = None
    
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
class CatalogExternalDataDTO:
    _id: Optional[int] = None
    _created_at: Optional[datetime] = None
    external_source_id: Optional[int] = None
    external_id: Optional[str] = None
    internal_id: Optional[int] = None
    internal_typeid: Optional[int] = None

@dataclass
class CatalogExternalSourceDTO(CatalogDTO):
    is_active: Optional[bool] = None
    last_sync_at: Optional[datetime] = None
