from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import ConfigManager, SupervisorConfig
from ..core import Supervisor
from ..database.session import get_session


async def get_supervisor(request: Request) -> Supervisor:
    return request.app.state.supervisor

SupervisorDeps = Annotated[Supervisor, Depends(get_supervisor)]

SessionDeps = Annotated[AsyncSession, Depends(get_session)]

ConfigDeps = Annotated[SupervisorConfig, Depends(ConfigManager.get_configs)]
