# -*- coding: utf-8 -*-
from __future__ import absolute_import
import logging

from functools import wraps
from typing import List, Optional, Dict, Any

from orchestrator import InventoryFilter, DeviceLightLoc, Completion
from orchestrator import ServiceDescription, DaemonDescription
from orchestrator import OrchestratorClientMixin, raise_if_exception, OrchestratorError
from orchestrator import HostSpec
from .. import mgr

logger = logging.getLogger('orchestrator')


# pylint: disable=abstract-method
class OrchestratorAPI(OrchestratorClientMixin):
    def __init__(self):
        super(OrchestratorAPI, self).__init__()
        self.set_mgr(mgr)  # type: ignore

    def status(self):
        try:
            status, desc = super(OrchestratorAPI, self).available()
            logger.info("is orchestrator available: %s, %s", status, desc)
            return dict(available=status, description=desc)
        except (RuntimeError, OrchestratorError, ImportError):
            return dict(
                available=False,
                description='Orchestrator is unavailable for unknown reason')

    def orchestrator_wait(self, completions):
        return self._orchestrator_wait(completions)


def wait_api_result(method):
    @wraps(method)
    def inner(self, *args, **kwargs):
        completion = method(self, *args, **kwargs)
        self.api.orchestrator_wait([completion])
        raise_if_exception(completion)
        return completion.result
    return inner


class ResourceManager(object):
    def __init__(self, api):
        self.api = api


class HostManger(ResourceManager):
    @wait_api_result
    def list(self) -> List[HostSpec]:
        return self.api.get_hosts()

    def get(self, hostname: str) -> Optional[HostSpec]:
        hosts = [host for host in self.list() if host.hostname == hostname]
        return hosts[0] if hosts else None

    @wait_api_result
    def add(self, hostname: str):
        return self.api.add_host(HostSpec(hostname))

    @wait_api_result
    def remove(self, hostname: str):
        return self.api.remove_host(hostname)

    @wait_api_result
    def add_label(self, host: str, label: str) -> Completion:
        return self.api.add_host_label(host, label)

    @wait_api_result
    def remove_label(self, host: str, label: str) -> Completion:
        return self.api.remove_host_label(host, label)


class InventoryManager(ResourceManager):
    @wait_api_result
    def list(self, hosts=None, refresh=False):
        host_filter = InventoryFilter(hosts=hosts) if hosts else None
        return self.api.get_inventory(host_filter=host_filter, refresh=refresh)


class ServiceManager(ResourceManager):
    @wait_api_result
    def list(self, service_name: Optional[str] = None) -> List[ServiceDescription]:
        return self.api.describe_service(None, service_name)

    @wait_api_result
    def get(self, service_name: str) -> ServiceDescription:
        return self.api.describe_service(None, service_name)

    @wait_api_result
    def list_daemons(self,
                     service_name: Optional[str] = None,
                     hostname: Optional[str] = None) -> List[DaemonDescription]:
        return self.api.list_daemons(service_name, host=hostname)

    def reload(self, service_type, service_ids):
        if not isinstance(service_ids, list):
            service_ids = [service_ids]

        completion_list = [
            self.api.service_action('reload', service_type, service_name,
                                    service_id)
            for service_name, service_id in service_ids
        ]
        self.api.orchestrator_wait(completion_list)
        for c in completion_list:
            raise_if_exception(c)


class OsdManager(ResourceManager):
    @wait_api_result
    def create(self, drive_group_specs):
        return self.api.apply_drivegroups(drive_group_specs)

    @wait_api_result
    def remove(self, osd_ids, replace=False, force=False):
        return self.api.remove_osds(osd_ids, replace, force)

    @wait_api_result
    def removing_status(self):
        return self.api.remove_osds_status()


class OrchClient(object):

    _instance = None

    @classmethod
    def instance(cls):
        # type: () -> OrchClient
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.api = OrchestratorAPI()

        self.hosts = HostManger(self.api)
        self.inventory = InventoryManager(self.api)
        self.services = ServiceManager(self.api)
        self.osds = OsdManager(self.api)

    def available(self, features: Optional[List[str]] = None) -> bool:
        available = self.status()['available']
        if available and features is not None:
            return not self.get_missing_features(features)
        return available

    def status(self) -> Dict[str, Any]:
        status = self.api.status()
        status['features'] = {}
        if status['available']:
            status['features'] = self.api.get_feature_set()
        return status

    def get_missing_features(self, features: List[str]) -> List[str]:
        supported_features = {k for k, v in self.api.get_feature_set().items() if v['available']}
        return list(set(features) - supported_features)

    @wait_api_result
    def blink_device_light(self, hostname, device, ident_fault, on):
        # type: (str, str, str, bool) -> Completion
        return self.api.blink_device_light(
            ident_fault, on, [DeviceLightLoc(hostname, device, device)])


class OrchFeature(object):
    HOST_LIST = 'get_hosts'
    HOST_CREATE = 'add_host'
    HOST_DELETE = 'remove_host'
    HOST_LABEL_ADD = 'add_host_label'
    HOST_LABEL_REMOVE = 'remove_host_label'

    SERVICE_LIST = 'describe_service'
    SERVICE_RELOAD = 'service_action'
    DAEMON_LIST = 'list_daemons'

    OSD_GET_REMOVE_STATUS = 'remove_osds_status'

    OSD_CREATE = 'apply_drivegroups'
    OSD_DELETE = 'remove_osds'

    DEVICE_LIST = 'get_inventory'
    DEVICE_BLINK_LIGHT = 'blink_device_light'
