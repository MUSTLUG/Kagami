import logging

# from sqlalchemy import select
import grpc

from ..config import ConfigManager
from ..database.database_service import WorkerService
from ..database.session import session_generator
from ..grpc import supervisor_pb2, supervisor_pb2_grpc, worker_pb2, worker_pb2_grpc
from .models import ProviderInfo, ResourceInfo, WorkerInfo
from .models.provider_info import ProviderStatus
from .models.resource_info import ResourceStatus
from .models.worker_info import WorkerStatus

logger = logging.getLogger(__name__)


class Supervisor(supervisor_pb2_grpc.SupervisorServicer):
    supervisor_addr: str

    resources: dict[str, ResourceInfo]  # resource_name -> ResourceInfo

    registered_workers: dict[str, WorkerInfo]  # worker_addr -> WorkerInfo
    unregistered_worker: list[str]  # list[worker_addr]

    def __init__(
        self,
        supervisor_host: str,
        supervisor_port: int,
        worker_info_list: list[WorkerInfo],
    ):
        super().__init__()
        self.supervisor_addr = self._parse_address(supervisor_host, supervisor_port)
        # build worker_info
        raw_worker_info = {}
        if worker_info_list:
            for worker_info in worker_info_list:
                raw_worker_info[worker_info.worker_addr] = worker_info
        self.registered_workers = raw_worker_info

        self.unregistered_worker = []

    @classmethod
    async def load(cls) -> "Supervisor":
        config = ConfigManager.get_configs()
        raw_sup = cls(
            supervisor_host=config.grpc_host,
            supervisor_port=config.grpc_port,
            worker_info_list=[],
        )
        # Load the worker from database (if not init)
        async with session_generator() as session:
            worker_service = WorkerService(session=session)
            workers = await worker_service.list_all_worker()

        logger.debug(f"Reconnect worker: {workers}")
        raw_worker_info_list: list[WorkerInfo] = []
        for worker in workers:
            worker_info = await raw_sup.connect_and_build_worker_info(
                worker_addr=worker.worker_addr
            )
            if worker_info is not None:
                raw_worker_info_list.append(worker_info)

        if raw_worker_info_list:
            raw_sup.registered_workers = {
                w.worker_addr: w for w in raw_worker_info_list
            }
            logger.debug(f"Load worker: {raw_worker_info_list}")
        else:
            logger.debug("No worker to load.")
        return raw_sup

    async def worker_report_in(self, request, context):
        """
        gRPC function
        worker_report_in()
        Recive worker report in request and add it into queue.
        """
        worker_addr = request.worker_addr
        # worker_status = request.worker_status
        logger.info(f"Recive worker report in: {worker_addr}")
        self.unregistered_worker.append(worker_addr)
        return supervisor_pb2.WorkerReportInResponse(
            supervisor_addr=self.supervisor_addr
        )

    async def update_provider_status(self, request, context):
        """
        gRPC function
        update_provider_status()
        Update provider's status after provider status had changed.
        """
        worker_addr = request.worker_addr
        provider_replica_id = request.provider_replica_id
        provider_status = request.provider_status
        worker = self.registered_workers.get(worker_addr)
        provider = worker.get_provider_by_replica_id(provider_replica_id)
        if provider is not None:
            provider.provider_status = ProviderStatus(provider_status)
            self.rebuild_resource_info(provider.name)
        else:
            logger.error(f"Provider not found for: {worker_addr}:{provider_replica_id}")

        return supervisor_pb2.UpdateProviderStatusResponse(
            provider_id=provider_replica_id
        )

    async def connect_and_build_worker_info(
        self, worker_addr: str
    ) -> WorkerInfo | None:
        """
        supervisor remote function
        connect_and_build_worker_info
        fetch workerinfo from worker, include provider info
        """
        providers = await self.get_providers(worker_addr)
        if providers != 1:
            worker_status = WorkerStatus.CONNECTED
            logger.info(f"Load worker {worker_addr} successfully")
        else:
            worker_status = WorkerStatus.DISCONNECTED
            logger.error(f"Fail to load worker: {worker_addr}")
        return WorkerInfo(
            worker_addr=worker_addr,
            worker_status=worker_status,
            providers=providers if isinstance(providers, list) else [],
        )

    async def save_worker_to_database(self, worker_addr: str):
        async with session_generator() as session:
            worker_service = WorkerService(session=session)
            await worker_service.add_workerinfo(worker_addr=worker_addr)
            logger.debug(f"Worker {worker_addr} recorded in database.")

    async def get_registered_worker(self) -> list[str]:
        return list(self.registered_workers.keys())

    async def get_unregistered_worker(self) -> list[str]:
        """
        supervisor function
        get_unregistered_worker()
        Get workers reported in and waiting in queue.
        """
        return self.unregistered_worker

    async def get_resource_status(self, name: str) -> ResourceStatus | None:
        """
        supervisor function
        get_resource_status()
        Get status of a resource.
        """
        resource = self.resources.get(name)
        resource_status = None
        if resource is not None:
            resource_status = resource.status
        else:
            logger.error(f"Could not get resource status of: {name}")
        return resource_status

    async def list_resource(self) -> dict[str, ResourceInfo]:
        """
        supervisor function
        list_resource()
        List all the resource record in supervisor
        """
        return self.resources

    async def get_resource_info(self, resource_name: str) -> ResourceInfo | None:
        """
        supervisor function
        get_resource_info()
        Get a resource info
        """
        resource = self.resources.get(resource_name)
        if not resource:
            logger.error(f"Resouce not found: {resource_name}")
        return resource

    @staticmethod
    async def get_providers(worker_addr: str) -> list[ProviderInfo] | int:
        """
        supervisor remote function()
        get_providers()
        Get all providers from a worker
        """
        async with grpc.aio.insecure_channel(worker_addr) as channel:
            stub = worker_pb2_grpc.WorkerStub(channel=channel)
            # TODO secure channel
            request = worker_pb2.GetProviderRequest(name=None)
            try:
                response = await stub.get_providers(request)
                return response.providers
            except grpc.RpcError as e:
                logger.exception(
                    f"Failed to get providers from worker: {worker_addr}, {e}"
                )
                return 0  # Failed to connect to worker

    async def check_worker_health(self, worker_addr: str):
        """
        supervisor remote function
        check_worker_health()
        Check worker's connectivity by exchanging supervisor_addr and worker_addr
        """
        logger.debug(f"Check worker health: {worker_addr}")
        async with grpc.aio.insecure_channel(worker_addr) as channel:
            stub = worker_pb2_grpc.WorkerStub(channel=channel)
            # TODO secure channel
            request = worker_pb2.HealthCheckRequest(
                supervisor_addr=self.supervisor_addr
            )
            try:
                # send register_accepted to worker with gRPC
                response = await stub.health_check(request)
                # check worker_addr in response
                assert worker_addr == response.worker_addr
                logger.debug(f"Health check successfully: {worker_addr}")
            except grpc.RpcError as e:
                logger.exception(
                    f"Failed to check health of worker: {worker_addr}, {e}"
                )
            except AssertionError as ae:
                logger.exception(
                    f"Worker Address not the same: {worker_addr}:"
                    f"{response.worker_addr}, {ae}"
                )

    async def regiser_worker(self, worker_addr: str):
        """
        supervisor remote function
        register_worker()
        Accept worker report in.
        """
        if worker_addr in self.unregistered_worker:
            logger.info(f"Accepted worker: {worker_addr}")
            async with grpc.aio.insecure_channel(worker_addr) as channel:
                stub = worker_pb2_grpc.WorkerStub(channel=channel)
                # TODO secure channel
                request = worker_pb2.RegisterResponse(accepted=True)
                try:


                    worker_info = await self.connect_and_build_worker_info(
                        worker_addr=worker_addr
                    )
                    if worker_info is not None:
                        self.registered_workers[worker_addr] = worker_info
                    else:
                        logger.error(f"Failed to register worker: {worker_addr}")
                        return
                    self.unregistered_worker.pop()
                    # send register_accepted to worker with gRPC
                    response = await stub.register_accepted(request)
                    await self.save_worker_to_database(worker_addr=worker_addr)
                    logger.info(f"Accepted register from worker: {worker_addr}")
                    logger.debug(f"Response: {response}")
                except grpc.RpcError as e:
                    logger.exception(
                        f"Failed to send register_accepted to worker: {worker_addr},{e}"
                    )

    async def rebuild_resource_info(self, resource_name: str) -> None:
        """
        supervisor function
        rebuild_resource_info()
        rebuild resource info from infomation provided by worker.
        maintain the resource_info since it does not store in database and worker.
        call update if resources or providers have changes.
        """
        raw_worker_info_list = []
        raw_provider_info_list = []
        provide_info_count = 0
        failed_providers_count = 0
        syncing_provider_count = 0
        raw_status = ResourceStatus.READY

        resource_info = self.resources.get(resource_name)

        for worker_info in self.registered_workers.values():
            provide_info = worker_info.providers.get(resource_name)
            if not provide_info:
                continue
            raw_worker_info_list.append(worker_info)
            raw_provider_info_list.append(provide_info)
            # check resource status
            provide_info_count += 1
            syncing_provider_count += (
                1 if provide_info.provider_status == ProviderStatus.SYNCING else 0
            )
            failed_providers_count += (
                1 if provide_info.provider_status == ProviderStatus.FAILED else 0
            )
        # check resource status
        # if all the providers of a resource are failed, status -> ERROR
        if failed_providers_count > 0:
            raw_status = (
                ResourceStatus.ERROR
                if failed_providers_count == provide_info_count
                else ResourceStatus.FAILED
            )
        elif syncing_provider_count > 0:
            raw_status = ResourceStatus.SYNCING

        if resource_info:
            resource_info.status = raw_status
            resource_info.update_workers(raw_worker_info_list)
            resource_info.update_providers(raw_provider_info_list)
        else:
            raw_resource_info = ResourceInfo(
                name=resource_name,
                status=raw_status,
                worker_info_list=raw_worker_info_list,
                provider_info_list=raw_provider_info_list,
                has_helper=True,  # TODO Helper
            )
            self.resources[resource_name] = raw_resource_info

    """
    supervisor function
    TODO update_resource_info()
    rebuild cost is high.
    """

    @staticmethod
    def _parse_address(host: str, port: int):
        return f"{host}:{port}"
