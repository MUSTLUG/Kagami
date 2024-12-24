import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

import grpc

from .config import ConfigManager
from .core import Supervisor
from .database.engine import DatabaseEngine
from .grpc import supervisor_pb2_grpc

config = ConfigManager.get_configs()
logger = logging.getLogger("uvicorn.error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"HTTP Host: {config.http_host}")
    logger.info(f"HTTP Port: {config.http_port}")
    logger.info(f"gRPC Host: {config.grpc_host}")
    logger.info(f"gRPC Port: {config.grpc_port}")
    logger.info(f"Log File: {config.log_file}")
    logger.info(f"Log Level: {config.log_level}")
    logger.info(f"Database Echo: {config.database_echo}")

    DatabaseEngine.init()
    await DatabaseEngine.check_and_create_database()

    supervisor = await Supervisor.load()
    app.state.supervisor = supervisor
    # start gRPC server
    grpc_server = grpc.aio.server()
    supervisor_pb2_grpc.add_SupervisorServicer_to_server(supervisor, grpc_server)
    grpc_server.add_insecure_port(supervisor.supervisor_addr)  # TODO secure channel
    await grpc_server.start()

    try:
        yield
    finally:
        logger.info("Shutting down gRPC server...")
        await grpc_server.stop(0)
        logger.info("gRPC server shut down successfully.")


kagami_server = FastAPI(lifespan=lifespan)

# Lazy load routers
from .routes import (  # noqa: E402
    admin_router,
    helper_router,
    resource_router,
    root_router,
)

# Include Router Area
kagami_server.include_router(helper_router)
kagami_server.include_router(resource_router)
kagami_server.include_router(root_router)
kagami_server.include_router(admin_router)
