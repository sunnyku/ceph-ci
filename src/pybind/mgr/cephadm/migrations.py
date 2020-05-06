import logging
from typing import List, TYPE_CHECKING, Iterator

from ceph.deployment.service_spec import PlacementSpec, ServiceSpec, HostPlacementSpec
from cephadm.inventory import Inventory, SpecStore, HostCache
from cephadm.schedule import HostAssignment

from orchestrator import DaemonDescription, OrchestratorError

if TYPE_CHECKING:
    from .module import CephadmOrchestrator

LAST_MIGRATION = 1

logger = logging.getLogger(__name__)



class Migrations:
    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr = mgr

        self.initialize()

    def initialize(self):
        if self.mgr.migration_current is None:
            self.set(0)
        self.migrate()

    def set(self, val):
        self.mgr.set_module_option('migration_current', val)
        self.mgr.migration_current = val

    def is_migration_ongoing(self):
        return self.mgr.migration_current != LAST_MIGRATION

    def verify_no_migration(self):
        if self.is_migration_ongoing():
            raise OrchestratorError("cephadm migration still ongoing. Please wait, until the migration is complete.")

    def migrate(self):
        if self.mgr.migration_current == 0:
            if self.migrate_0_1():
                self.set(1)

    def migrate_0_1(self):
        """
        Migration 0 -> 1
        New scheduler that takes PlacementSpec as the bound and not as recommendation.
        It won't suggest any new placements outside of the hosts specified by label etc.

        Which means, we have to make sure, we're not removing any daemons directly after
        upgrading to the new scheduler.

        There is a potential race here:
        1. user updates his spec to remove daemons
        2. mgr gets upgraded to new scheduler, before the old scheduler removed the daemon
        3. now, we're converting the spec to explicit placement, thus reverting (1.)
        I think this is ok.
        """

        def interesting_specs() -> Iterator[ServiceSpec]:
            for s in self.mgr.spec_store.specs.values():
                if s.unmanaged:
                    continue
                p = s.placement
                if p is None:
                    continue
                if p.count is None:
                    continue
                if not p.hosts and not p.host_pattern and not p.label:
                    continue
                yield s

        def convert_to_explicit(spec: ServiceSpec):
            placements = HostAssignment(
                spec=spec,
                get_hosts_func=self.mgr._get_hosts,
                get_daemons_func=self.mgr.cache.get_daemons_by_service
            ).place()

            existing_daemons = self.mgr.cache.get_daemons_by_service(spec.service_name())

            # We have to migrate, only if the new scheduler would remove daemons
            if len(placements) >= len(existing_daemons):
                return

            old_hosts = {h.hostname: h for h in spec.placement.hosts}
            new_hosts = [
                old_hosts[d.hostname] if d.hostname in old_hosts else HostPlacementSpec(hostname=d.hostname, network='', name='')
                for d in existing_daemons
            ]

            new_placement = PlacementSpec(
                hosts=new_hosts,
                count=spec.placement.count
            )

            new_spec = ServiceSpec.from_json(spec.to_json())
            new_spec.placement = new_placement

            logger.info(f"Migrating {spec.one_line_str()} to explicit placement")

            self.mgr.spec_store.save(new_spec)

        specs = list(interesting_specs())
        if not specs:
            return True  # nothing to do. shortcut

        if not self.mgr.cache.daemon_cache_filled():
            logger.info("Unable to migrate yet. Daemon Cache still incomplete.")
            return False

        for spec in specs:
            convert_to_explicit(spec)

        return True
