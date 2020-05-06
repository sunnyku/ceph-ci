from typing import List, Optional
import cherrypy

from . import ApiController, RESTController
from .orchestrator import raise_if_no_orchestrator
from ..security import Scope
from ..services.orchestrator import OrchClient, OrchFeature


@ApiController('/service', Scope.HOSTS)
class Service(RESTController):

    @raise_if_no_orchestrator([OrchFeature.SERVICE_LIST])
    def list(self, service_name: Optional[str] = None) -> List[dict]:
        orch = OrchClient.instance()
        return [service.to_json() for service in orch.services.list(service_name)]

    @raise_if_no_orchestrator([OrchFeature.SERVICE_LIST])
    def get(self, service_name: str) -> List[dict]:
        orch = OrchClient.instance()
        services = orch.services.get(service_name)
        if not services:
            raise cherrypy.HTTPError(404, 'Service {} not found'.format(service_name))
        return services[0].to_json()

    @RESTController.Resource('GET')
    @raise_if_no_orchestrator([OrchFeature.DAEMON_LIST])
    def daemons(self, service_name: str) -> List[dict]:
        orch = OrchClient.instance()
        daemons = orch.services.list_daemons(service_name)
        return [d.to_json() for d in daemons]
