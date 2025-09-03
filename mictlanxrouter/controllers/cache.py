from fastapi import APIRouter,HTTPException,Response
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from mictlanx.logger.log import Log
from mictlanxrouter.caching import CacheX
from opentelemetry.trace import Tracer,Status,StatusCode
class CacheController:
    def __init__(self,log:Log, cache:CacheX,tracer:Tracer


                 ):
        self.router = APIRouter()
        self.log=log
        self.cache= cache
        self.add_routes()
    def add_routes(self):
        @self.router.get("/api/v4/cache")
        def get_cache():
            try:
                return JSONResponse(
                    jsonable_encoder({
                        "keys":self.cache.get_keys()
                    })
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        @self.router.get("/api/v4/cache/stats")
        def get_cache_stats():
            try:
                return JSONResponse(
                    jsonable_encoder({
                        "total":self.cache.get_total_storage_capacity(),
                        "used":self.cache.get_used_storage_capacity(),
                        "available": self.cache.get_total_storage_capacity() - self.cache.get_used_storage_capacity(),
                        "uf": self.cache.get_uf()
                    })
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        @self.router.delete("/api/v4/cache/reset")
        def reset_cache():
            try:
                self.cache.clear()
                return Response(content=None, status_code=204)
                # return JSONResponse(
                #     jsonable_encoder({
                #         "total":self.cache.get_total_storage_capacity(),
                #         "used":self.cache.get_used_storage_capacity(),
                #         "available": self.cache.get_total_storage_capacity() - self.cache.get_used_storage_capacity(),
                #         "uf": self.cache.get_uf()
                #     })
                # )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))