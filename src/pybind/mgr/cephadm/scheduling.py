import logging
import random
from typing import List, Optional, Callable, Tuple, Any

from orchestrator import OrchestratorValidationError, PlacementSpec, HostPlacementSpec, ServiceSpec

logger = logging.getLogger(__name__)




class BaseScheduler(object):
    """
    Base Scheduler Interface

    * requires a placement_spec

    `place(host_pool)` needs to return a List[HostPlacementSpec, ..]
    """

    def __init__(self, placement_spec):
        # type: (PlacementSpec) -> None
        self.placement_spec = placement_spec

    def place(self, host_pool, count=None):
        # type: (List, Optional[int]) -> List[HostPlacementSpec]
        raise NotImplementedError


class SimpleScheduler(BaseScheduler):
    """
    The most simple way to pick/schedule a set of hosts.
    1) Shuffle the provided host_pool
    2) Select from list up to :count
    """
    def __init__(self, placement_spec):
        super(SimpleScheduler, self).__init__(placement_spec)

    def place(self, host_pool, count=None):
        # type: (List, Optional[int]) -> List[HostPlacementSpec]
        if not host_pool:
            raise Exception('List of host candidates is empty')
        host_pool = [HostPlacementSpec(x, '', '') for x in host_pool]
        # shuffle for pseudo random selection
        random.shuffle(host_pool)
        return host_pool[:count]


class NodeAssignment(object):
    """
    A class to detect if nodes are being passed imperative or declarative
    If the spec is populated via the `nodes/hosts` field it will not load
    any nodes into the list.
    If the spec isn't populated, i.e. when only num or label is present (declarative)
    it will use the provided `get_host_func` to load it from the inventory.

    Schedulers can be assigned to pick hosts from the pool.
    """

    def __init__(self,
                 spec,  # type: ServiceSpec
                 get_hosts_func,  # type: Callable[[], List[Tuple[str, Any]]]
                 service_type,  # type: str
                 scheduler=None,  # type: Optional[BaseScheduler]
                 ):
        self.spec = spec  # type: ServiceSpec
        self.scheduler = scheduler if scheduler else SimpleScheduler(self.spec.placement)
        self.get_hosts_func = get_hosts_func
        self.service_type = service_type

    def load(self):
        # type: () -> ServiceSpec
        """
        Load nodes into the spec.placement.nodes container.
        """
        self.load_labeled_nodes()
        self.assign_nodes()
        self.validate_placement()
        return self.spec

    def load_labeled_nodes(self):
        # type: () -> None
        """
        Assign nodes based on their label
        """
        # Querying for labeled nodes doesn't work currently.
        # Leaving this open for the next iteration
        # NOTE: This currently queries for all hosts without label restriction
        if self.spec.placement.label:
            logger.info("Found labels. Assigning nodes that match the label")
            candidates = [HostPlacementSpec(x[0], '', '') for x in self.get_hosts_func()]  # TODO: query for labels
            logger.info('Assigning nodes to spec: {}'.format(candidates))
            self.spec.placement.set_hosts(candidates)

    def assign_nodes(self):
        # type: () -> None
        """
        Use the assigned scheduler to load nodes into the spec.placement.nodes container
        """
        # If no imperative or declarative host assignments, use the scheduler to pick from the
        # host pool (assuming `count` is set)
        if not self.spec.placement.label and not self.spec.placement.hosts and self.spec.placement.count:
            logger.info("Found num spec. Looking for labeled nodes.")
            # TODO: actually query for labels (self.service_type)
            candidates = self.scheduler.place([x[0] for x in self.get_hosts_func()],
                                              count=self.spec.placement.count)
            # Not enough nodes to deploy on
            if len(candidates) != self.spec.placement.count:
                logger.warning("Did not find enough labeled nodes to \
                               scale to <{}> daemons. Falling back to unlabeled nodes.".
                               format(self.spec.placement.count))
            else:
                logger.info('Assigning nodes to spec: {}'.format(candidates))
                self.spec.placement.set_hosts(candidates)
                return None

            candidates = self.scheduler.place([x[0] for x in self.get_hosts_func()], count=self.spec.placement.count)
            # Not enough nodes to deploy on
            if len(candidates) != self.spec.placement.count:
                raise OrchestratorValidationError("Cannot place {} daemons on {} hosts.".
                                                  format(self.spec.placement.count, len(candidates)))

            logger.info('Assigning nodes to spec: {}'.format(candidates))
            self.spec.placement.set_hosts(candidates)
            return None
        else:
            logger.info('Using hosts: {}'.format(self.spec.placement.hosts))

    def validate_placement(self):
        count = self.spec.placement.count
        if count is None:
            raise OrchestratorValidationError(
                "must specify a number of daemons (count)")
        if count <= 0:
            raise OrchestratorValidationError(
                "number of daemons (count) must be at least 1")

        if not self.spec.placement.hosts or len(self.spec.placement.hosts) < count:
            raise OrchestratorValidationError("must specify at least %d hosts" % count)
