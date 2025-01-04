import asyncio
import json
import logging
from collections.abc import Sequence
from pathlib import Path

import grpc

from ..config import ConfigManager
from ..core.provider import PROVIDER_CLASS_MAP, BaseProvider
from ..grpc import supervisor_pb2, supervisor_pb2_grpc, worker_pb2, worker_pb2_grpc

logger = logging.getLogger(__name__)


class Worker(worker_pb2_grpc.WorkerServicer):
    worker_addr: str
    supervisor_addr: str
    registered: bool

    providers: dict[str, BaseProvider]  # name -> Provider

    def __init__(
        self,
        worker_host: str,
        worker_port: int,
        supervisor_host: str,
        supervisor_port: int,
        registerd: bool,
        providers: Sequence[BaseProvider],
    ):
        super().__init__()
        self.worker_addr = self._parse_address(worker_host, worker_port)
        self.supervisor_addr = self._parse_address(supervisor_host, supervisor_port)
        assert self.worker_addr and self.supervisor_addr

        self.registered = registerd
        self.providers = {}
        if providers:
            for provider in providers:
                self.providers[provider.name] = provider

    @classmethod
    def load(
        cls,
        worker_host: str,
        worker_port: int,
        supervisor_host: str,
        supervisor_port: int,
        config_folder: Path,
    ) -> "Worker":
        provider_info_list = []
        logger.info(f"Read provider configs from: {config_folder}")
        for provider_file in config_folder.glob("*.json"):
            if provider_file:
                try:
                    provider_json = json.loads(provider_file.read_bytes())
                    cls_type = PROVIDER_CLASS_MAP[provider_json.get("provider_method")]
                    provider = cls_type(**provider_json)
                    provider_info_list.append(provider)
                except Exception as e:
                    logger.exception(f"Read provider config error: {e}")

        # check if registered
        registered = (config_folder / "registered").exists()
        return Worker(
            worker_host=worker_host,
            worker_port=worker_port,
            supervisor_host=supervisor_host,
            supervisor_port=supervisor_port,
            registerd=registered,
            providers=provider_info_list,
        )

    """
    gRPC function
    sync_from_upstream()
    Sync repo from upstream. Implemented for remote call.
    """

    async def sync_from_upstream(self, request, context):
        source_name = request.name
        logger.info(f"Syncing request from supervisor: {source_name}")

        # search provider for {source_name}
        provider = self._get_provider(source_name)
        return_code = 1
        msg = ""
        if provider:
            return_code = await provider.sync_from_upstream()
            if return_code == 0:
                msg = "Success"
        else:
            logger.error(f"No provider found for {source_name}")
            msg = "Failed"

        return worker_pb2.SyncResponse(status=return_code, message=msg)

    """
    gRPC function
    accept_register()
    Callback function for register request was accepted by supervisor.
    """

    async def register_accepted(self, request, context):
        accepted = request.accepted
        if accepted:
            self.registered = True
            message = "Worker Registered"

        logger.info("Worker Registered.")
        # create registered note
        config = ConfigManager.get_configs()
        registered_file = Path(config.config_folder) / "registered"
        registered_file.touch()

        return worker_pb2.RegisterAck(message=message)

    """
    gRPC function
    health_check()
    For supervisor to check worker health by exchanging address.
    """

    async def health_check(self, request, context):
        try:
            assert request.supervisor_addr == self.supervisor_addr
            return worker_pb2.HealthCheckResponse(worker_addr=self.worker_addr)
        except AssertionError as ae:
            logger.error(
                f"Supervisor Address not match: {request.supervisor_addr}, {ae}"
            )

    """
    gRPC function
    get_providers()
    """

    async def get_providers(self, request, context):
        raw_provider_info = []
        providers = self._get_provider(name=(request.name if request.name else None))
        raw_provider_info = [
            worker_pb2.ProviderInfo(
                name=provider.name,
                replica_id=provider.replica_id,
                upstreamurl=provider.upstreamurl,
                status=worker_pb2.ProviderStatus.Value(provider.status.name),
            )
            for provider in providers
        ]

        return worker_pb2.GetProviderResponse(providers=raw_provider_info)

    """
    worker remote function
    report_in()
    Send register request to supervisor (first connect).
    """

    async def report_in(self):
        while not self.registered:
            await self._register_report_in()
            await asyncio.sleep(15)

    async def _register_report_in(self):
        if not self.registered:
            # create channel
            # TODO secure channel
            async with grpc.aio.insecure_channel(self.supervisor_addr) as channel:
                stub = supervisor_pb2_grpc.SupervisorStub(channel=channel)
                # TODO send provider infos
                # provider_names = list(self.providers.keys())
                request = supervisor_pb2.WorkerReportInRequest(
                    worker_addr=self.worker_addr
                )
                logger.debug(f"Request: {request}")

                try:
                    # send report_in request with gRPC
                    response = await stub.worker_report_in(request)
                    logger.info(
                        f"Worker sent report_in to supervisor {self.supervisor_addr}"
                    )
                    logger.debug(f"Response: {response}")
                except grpc.RpcError as e:
                    logger.exception(
                        f"Failed to send report in worker: {self.worker_addr}, {e}"
                    )

    """
    worker remote function
    update_provider_status()
    Update provider status to supervisor, using gRPC call.
    Call this when provider status has changed.
    """

    async def update_provider_status(self, name: str):
        logger.info(f"Update provider status for {name}")
        provider = self._get_provider(name)[0]
        provider_status = provider.provider_status.value if provider else None
        replica_id = provider.replica_id if provider else None
        # TODO secure channel
        async with grpc.aio.insecure_channel(self.supervisor_addr) as channel:
            stub = supervisor_pb2_grpc.SupervisorStub(channel=channel)
            request = supervisor_pb2.UpdateProviderRequest(
                provider_replica_id=replica_id,
                provider_status=provider_status,
            )
            try:
                # send update_provider_status request with gRPC
                response = stub.update_provider_status(request)
                logger.info(f"Provider: {name} sent update status")
                logger.debug(f"Response: {response}")
            except grpc.RpcError as e:
                logger.exception(f"Failed to send update for provider: {name}: {e}")

    """
    utils
    get_provider()
    Return a provider according to the name
    """

    def _get_provider(self, name: str | None = None) -> list[BaseProvider]:
        if name:
            provider = self.providers.get(name)
            return [provider] if provider else []
        else:
            return list(self.providers.values())

    """
    utils
    _parse_address()
    Return full address
    """

    @staticmethod
    def _parse_address(host: str, port: int):
        return f"{host}:{port}"
