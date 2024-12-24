import logging

from fastapi import APIRouter
from pydantic import BaseModel

from .deps import SupervisorDeps

logger = logging.getLogger(__name__)
admin_router: APIRouter = APIRouter(prefix="/admin")

class ListWorkerResponse(BaseModel):
    worker_addr: str
    registered: bool

@admin_router.get("/list", response_model=list[ListWorkerResponse])
async def list_worker(supervisior: SupervisorDeps):
    registered_workers = await supervisior.get_registered_worker()
    unregistered_workrers = await supervisior.get_unregistered_worker()
    response = [
        {
            "worker_addr": worker_addr,
            "registered": worker_addr in registered_workers
        }
        for worker_addr in set(registered_workers + unregistered_workrers)
    ]
    return response
