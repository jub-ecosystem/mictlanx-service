from pydantic import BaseModel
from typing import Optional, Dict, Any
class MictlanxRouterError(BaseModel):
    code: int
    error_id:str
    message: str
    metadata: Optional[Dict[str, Any]] = {}

class NotFoundError(MictlanxRouterError):
    code: int = 404
    error_id: str = "NOT_FOUND"
    message: str = "The requested resource was not found."
    metadata: Optional[Dict[str, Any]] = {}