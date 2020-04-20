import logging
import random
from typing import List, Optional, Callable, Iterable, Tuple

import orchestrator
from ceph.deployment.service_spec import PlacementSpec, HostPlacementSpec, ServiceSpec
from orchestrator import OrchestratorValidationError

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
            return []
        host_pool = [x for x in host_pool]
        # shuffle for pseudo random selection
        random.shuffle(host_pool)
        return host_pool[:count]


class HostAssignment(object):
    """
    A class to detect if hosts are being passed imperative or declarative
    If the spec is populated via the `hosts/hosts` field it will not load
    any hosts into the list.
    If the spec isn't populated, i.e. when only num or label is present (declarative)
    it will use the provided `get_host_func` to load it from the inventory.

    Schedulers can be assigned to pick hosts from the pool.
    """

    def __init__(self,
                 spec,  # type: ServiceSpec
                 get_hosts_func,  # type: Callable[[Optional[str]],List[str]]
                 get_daemons_func, # type: Callable[[str],List[orchestrator.DaemonDescription]]

                 filter_new_host=None, # type: Optional[Callable[[str],bool]]
                 scheduler=None,  # type: Optional[BaseScheduler]
                 ):
        assert spec and get_hosts_func and get_daemons_func
        self.spec = spec  # type: ServiceSpec
        self.scheduler = scheduler if scheduler else SimpleScheduler(self.spec.placement)
        self.get_hosts_func = get_hosts_func
        self.get_daemons_func = get_daemons_func
        self.filter_new_host = filter_new_host
        self.service_name = spec.service_name()


    def validate(self):
        self.spec.validate()

        if self.spec.placement.hosts:
            explicit_hostnames = {h.hostname for h in self.spec.placement.hosts}
            unknown_hosts = explicit_hostnames.difference(set(self.get_hosts_func(None)))
            if unknown_hosts:
                raise OrchestratorValidationError(
                    f'Cannot place {self.spec.one_line_str()} on {", ".join(sorted(unknown_hosts))}: Unknown hosts')

        if self.spec.placement.host_pattern:
            pattern_hostnames = self.spec.placement.pattern_matches_hosts(self.get_hosts_func(None))
            if not pattern_hostnames:
                raise OrchestratorValidationError(
                    f'Cannot place {self.spec.one_line_str()}: No matching hosts')

        if self.spec.placement.label:
            label_hostnames = self.get_hosts_func(self.spec.placement.label)
            if not label_hostnames:
                raise OrchestratorValidationError(
                    f'Cannot place {self.spec.one_line_str()}: No matching '
                    f'hosts for label {self.spec.placement.label}')

    def place(self):
        # type: () -> List[HostPlacementSpec]
        """
        Load hosts into the spec.placement.hosts container.
        """

        self.validate()

        count = self.spec.placement.count
        assert count != 0

        hosts = self.get_candidates()
        if count is None:
            logger.debug('Provided hosts: %s' % self.spec.placement.hosts)
            return hosts

        chosen = hosts


        chosen, existing = self.prefer_hosts_with_daemons(chosen)

        need = count - len(existing)
        others = difference_hostspecs(chosen, existing)

        if need < 0:
            return self.scheduler.place(existing, count)
        else:
            if self.filter_new_host:
                old = others
                others = [h for h in others if self.filter_new_host(h.hostname)]
                logger.debug('filtered %s down to %s' % (old, hosts))

            chosen = self.scheduler.place(others, need)
            logger.debug('Combine hosts with existing daemons %s + new hosts %s' % (
                existing, chosen))
            return list(merge_hostspecs(existing, chosen))

    def get_candidates(self) -> List[HostPlacementSpec]:
        if self.spec.placement.hosts:
            return self.spec.placement.hosts
        elif self.spec.placement.label:
            return [
                HostPlacementSpec(x, '', '')
                for x in self.get_hosts_func(self.spec.placement.label)
            ]
        elif self.spec.placement.host_pattern:
            return [
                HostPlacementSpec(x, '', '')
                for x in self.spec.placement.pattern_matches_hosts(self.get_hosts_func(None))
            ]
        if self.spec.placement.count is None:
            raise OrchestratorValidationError("placement spec is empty: no hosts, no label, no pattern, no count")
        # backward compatibility: consider an empty placements to be the same pattern = *
        return [
            HostPlacementSpec(x, '', '')
            for x in self.get_hosts_func(None)
        ]

    def prefer_hosts_with_daemons(self, chosen: List[HostPlacementSpec]) -> Tuple[List[HostPlacementSpec], List[HostPlacementSpec]]:
        # prefer hosts that already have services
        daemons = self.get_daemons_func(self.service_name)
        hosts_with_daemons = {d.hostname for d in daemons}
        # calc existing daemons (that aren't already in chosen)
        existing = [hs for hs in chosen if hs.hostname in hosts_with_daemons]
        count = self.spec.placement.count
        assert count is not None
        if len(list(merge_hostspecs(chosen, existing))) >= count:
            chosen = list(merge_hostspecs(chosen, self.scheduler.place(
                existing,
                count - len(chosen))))
            logger.debug('Hosts with existing daemons: {}'.format(chosen))
        return chosen, existing


def merge_hostspecs(l: List[HostPlacementSpec], r: List[HostPlacementSpec]) -> Iterable[HostPlacementSpec]:
    l_names = {h.hostname for h in l}
    yield from l
    yield from (h for h in r if h.hostname not in l_names)


def difference_hostspecs(l: List[HostPlacementSpec], r: List[HostPlacementSpec]) -> List[HostPlacementSpec]:
    r_names = {h.hostname for h in r}
    return [h for h in l if h.hostname not in r_names]


