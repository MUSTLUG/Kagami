import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .deps import SupervisorDeps

logger = logging.getLogger(__name__)
admin_router: APIRouter = APIRouter(prefix="/admin")


class ListWorkerResponse(BaseModel):
    worker_addr: str
    registered: bool


class AcceptWorkerResponse(BaseModel):
    message: str


@admin_router.get("/list", response_model=list[ListWorkerResponse])
async def list_worker(supervisior: SupervisorDeps):
    registered_workers = await supervisior.get_registered_worker()
    unregistered_workrers = await supervisior.get_unregistered_worker()
    logger.debug(
        f"registered: {registered_workers}, unregistered: {unregistered_workrers}"
    )
    response = [
        {"worker_addr": worker_addr, "registered": worker_addr in registered_workers}
        for worker_addr in set(registered_workers + unregistered_workrers)
    ]
    return response


@admin_router.get("/{worker_addr}/accept", response_model=AcceptWorkerResponse)
async def accept_worker(worker_addr: str, supervisor: SupervisorDeps):
    try:
        await supervisor.regiser_worker(worker_addr=worker_addr)
        logger.info(f"Recived accept request for worker: {worker_addr}")
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Register worker failed")
    return {"message": f"Register worker {worker_addr} successfully"}
