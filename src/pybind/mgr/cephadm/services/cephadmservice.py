import json
import logging
from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, List, Callable, Any

from mgr_module import MonCommandFailed

from ceph.deployment.service_spec import ServiceSpec, RGWSpec
from orchestrator import OrchestratorError, DaemonDescription
from cephadm import utils

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class CephadmService(metaclass=ABCMeta):
    """
    Base class for service types. Often providing a create() and config() fn.
    """

    @property
    @abstractmethod
    def TYPE(self):
        pass

    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr

    def daemon_check_post(self, daemon_descrs: List[DaemonDescription]):
        """The post actions needed to be done after daemons are checked"""
        if self.mgr.config_dashboard:
            self.config_dashboard(daemon_descrs)
    
    def config_dashboard(self, daemon_descrs: List[DaemonDescription]):
        """Config dashboard settings."""
        raise NotImplementedError()

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        raise NotImplementedError()

    def _inventory_get_addr(self, hostname: str):
        """Get a host's address with its hostname."""
        return self.mgr.inventory.get_addr(hostname)

    def _set_service_url_on_dashboard(self,
                                      service_name: str,
                                      get_mon_cmd: str,
                                      set_mon_cmd: str,
                                      service_url: str):
        """A helper to get and set service_url via Dashboard's MON command.

           If result of get_mon_cmd differs from service_url, set_mon_cmd will
           be sent to set the service_url.
        """
        def get_set_cmd_dicts(out: str) -> List[dict]:
            cmd_dict = {
                'prefix': set_mon_cmd,
                'value': service_url
            }
            return [cmd_dict] if service_url != out else []

        self._check_and_set_dashboard(
            service_name=service_name,
            get_cmd=get_mon_cmd,
            get_set_cmd_dicts=get_set_cmd_dicts
        )

    def _check_and_set_dashboard(self,
                                 service_name: str,
                                 get_cmd: str,
                                 get_set_cmd_dicts: Callable[[str], List[dict]]):
        """A helper to set configs in the Dashboard.

        The method is useful for the pattern:
            - Getting a config from Dashboard by using a Dashboard command. e.g. current iSCSI
              gateways.
            - Parse or deserialize previous output. e.g. Dashboard command returns a JSON string.
            - Determine if the config need to be update. NOTE: This step is important because if a
              Dashboard command modified Ceph config, cephadm's config_notify() is called. Which
              kicks the serve() loop and the logic using this method is likely to be called again.
              A config should be updated only when needed.
            - Update a config in Dashboard by using a Dashboard command.

        :param service_name: the service name to be used for logging
        :type service_name: str
        :param get_cmd: Dashboard command prefix to get config. e.g. dashboard get-grafana-api-url
        :type get_cmd: str
        :param get_set_cmd_dicts: function to create a list, and each item is a command dictionary.
            e.g.
            [
                {
                   'prefix': 'dashboard iscsi-gateway-add',
                   'service_url': 'http://admin:admin@aaa:5000',
                   'name': 'aaa'
                },
                {
                    'prefix': 'dashboard iscsi-gateway-add',
                    'service_url': 'http://admin:admin@bbb:5000',
                    'name': 'bbb'
                }
            ]
            The function should return empty list if no command need to be sent.
        :type get_set_cmd_dicts: Callable[[str], List[dict]]
        """

        try:
            _, out, _ = self.mgr.check_mon_command({
                'prefix': get_cmd
            })
        except MonCommandFailed as e:
            logger.warning('Failed to get Dashboard config for %s: %s', service_name, e)
            return
        cmd_dicts = get_set_cmd_dicts(out.strip())
        for cmd_dict in list(cmd_dicts):
            try:
                logger.info('Setting Dashboard config for %s: command: %s', service_name, cmd_dict)
                _, out, _ = self.mgr.check_mon_command(cmd_dict)
            except MonCommandFailed as e:
                logger.warning('Failed to set Dashboard config for %s: %s', service_name, e)



    def ok_to_stop(self, daemon_ids: List[str]) -> bool:
        names = [f'{self.TYPE}.{d_id}' for d_id in daemon_ids]

        if self.TYPE not in ['mon', 'osd', 'mds']:
            logger.info('Upgrade: It is presumed safe to stop %s' % names)
            return True

        ret, out, err = self.mgr.mon_command({
            'prefix': f'{self.TYPE} ok-to-stop',
            'ids': daemon_ids,
        })

        if ret:
            logger.info(f'It is NOT safe to stop {names}: {err}')
            return False

        return True

    def pre_remove(self, daemon_id: str) -> None:
        """
        Called before the daemon is removed.
        """
        pass

class MonService(CephadmService):
    TYPE = 'mon'

    def create(self, name, host, network):
        """
        Create a new monitor on the given host.
        """
        # get mon. key
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get',
            'entity': 'mon.',
        })

        extra_config = '[mon.%s]\n' % name
        if network:
            # infer whether this is a CIDR network, addrvec, or plain IP
            if '/' in network:
                extra_config += 'public network = %s\n' % network
            elif network.startswith('[v') and network.endswith(']'):
                extra_config += 'public addrv = %s\n' % network
            elif ':' not in network:
                extra_config += 'public addr = %s\n' % network
            else:
                raise OrchestratorError('Must specify a CIDR network, ceph addrvec, or plain IP: \'%s\'' % network)
        else:
            # try to get the public_network from the config
            ret, network, err = self.mgr.check_mon_command({
                'prefix': 'config get',
                'who': 'mon',
                'key': 'public_network',
            })
            network = network.strip() # type: ignore
            if not network:
                raise OrchestratorError('Must set public_network config option or specify a CIDR network, ceph addrvec, or plain IP')
            if '/' not in network:
                raise OrchestratorError('public_network is set but does not look like a CIDR network: \'%s\'' % network)
            extra_config += 'public network = %s\n' % network

        return self.mgr._create_daemon('mon', name, host,
                                       keyring=keyring,
                                       extra_config={'config': extra_config})

    def _check_safe_to_destroy(self, mon_id):
        # type: (str) -> None
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'quorum_status',
        })
        try:
            j = json.loads(out)
        except Exception as e:
            raise OrchestratorError('failed to parse quorum status')

        mons = [m['name'] for m in j['monmap']['mons']]
        if mon_id not in mons:
            logger.info('Safe to remove mon.%s: not in monmap (%s)' % (
                mon_id, mons))
            return
        new_mons = [m for m in mons if m != mon_id]
        new_quorum = [m for m in j['quorum_names'] if m != mon_id]
        if len(new_quorum) > len(new_mons) / 2:
            logger.info('Safe to remove mon.%s: new quorum should be %s (from %s)' % (mon_id, new_quorum, new_mons))
            return
        raise OrchestratorError('Removing %s would break mon quorum (new quorum %s, new mons %s)' % (mon_id, new_quorum, new_mons))


    def pre_remove(self, daemon_id: str) -> None:
        self._check_safe_to_destroy(daemon_id)

        # remove mon from quorum before we destroy the daemon
        logger.info('Removing monitor %s from monmap...' % daemon_id)
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'mon rm',
            'name': daemon_id,
        })


class MgrService(CephadmService):
    TYPE = 'mgr'

    def create(self, mgr_id, host):
        """
        Create a new manager instance on a host.
        """
        # get mgr. key
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'mgr.%s' % mgr_id,
            'caps': ['mon', 'profile mgr',
                     'osd', 'allow *',
                     'mds', 'allow *'],
        })

        return self.mgr._create_daemon('mgr', mgr_id, host, keyring=keyring)


class MdsService(CephadmService):
    TYPE = 'mds'

    def config(self, spec: ServiceSpec):
        # ensure mds_join_fs is set for these daemons
        assert spec.service_id
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'config set',
            'who': 'mds.' + spec.service_id,
            'name': 'mds_join_fs',
            'value': spec.service_id,
        })

    def create(self, mds_id, host) -> str:
        # get mgr. key
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'mds.' + mds_id,
            'caps': ['mon', 'profile mds',
                     'osd', 'allow rwx',
                     'mds', 'allow'],
        })
        return self.mgr._create_daemon('mds', mds_id, host, keyring=keyring)


class RgwService(CephadmService):
    TYPE = 'rgw'

    def config(self, spec: RGWSpec):
        # ensure rgw_realm and rgw_zone is set for these daemons
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'config set',
            'who': f"{utils.name_to_config_section('rgw')}.{spec.service_id}",
            'name': 'rgw_zone',
            'value': spec.rgw_zone,
        })
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'config set',
            'who': f"{utils.name_to_config_section('rgw')}.{spec.rgw_realm}",
            'name': 'rgw_realm',
            'value': spec.rgw_realm,
        })
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'config set',
            'who': f"{utils.name_to_config_section('rgw')}.{spec.service_id}",
            'name': 'rgw_frontends',
            'value': spec.rgw_frontends_config_value(),
        })

        if spec.rgw_frontend_ssl_certificate:
            if isinstance(spec.rgw_frontend_ssl_certificate, list):
                cert_data = '\n'.join(spec.rgw_frontend_ssl_certificate)
            elif isinstance(spec.rgw_frontend_ssl_certificate, str):
                cert_data = spec.rgw_frontend_ssl_certificate
            else:
                raise OrchestratorError(
                        'Invalid rgw_frontend_ssl_certificate: %s'
                        % spec.rgw_frontend_ssl_certificate)
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config-key set',
                'key': f'rgw/cert/{spec.rgw_realm}/{spec.rgw_zone}.crt',
                'val': cert_data,
            })

        if spec.rgw_frontend_ssl_key:
            if isinstance(spec.rgw_frontend_ssl_key, list):
                key_data = '\n'.join(spec.rgw_frontend_ssl_key)
            elif isinstance(spec.rgw_frontend_ssl_certificate, str):
                key_data = spec.rgw_frontend_ssl_key
            else:
                raise OrchestratorError(
                        'Invalid rgw_frontend_ssl_key: %s'
                        % spec.rgw_frontend_ssl_key)
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config-key set',
                'key': f'rgw/cert/{spec.rgw_realm}/{spec.rgw_zone}.key',
                'val': key_data,
            })

        logger.info('Saving service %s spec with placement %s' % (
            spec.service_name(), spec.placement.pretty_str()))
        self.mgr.spec_store.save(spec)

    def create(self, rgw_id, host) -> str:
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': f"{utils.name_to_config_section('rgw')}.{rgw_id}",
            'caps': ['mon', 'allow *',
                     'mgr', 'allow rw',
                     'osd', 'allow rwx'],
        })
        return self.mgr._create_daemon('rgw', rgw_id, host, keyring=keyring)


class RbdMirrorService(CephadmService):
    TYPE = 'rbd-mirror'

    def create(self, daemon_id, host) -> str:
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'client.rbd-mirror.' + daemon_id,
            'caps': ['mon', 'profile rbd-mirror',
                     'osd', 'profile rbd'],
        })
        return self.mgr._create_daemon('rbd-mirror', daemon_id, host,
                                       keyring=keyring)


class CrashService(CephadmService):
    TYPE = 'crash'

    def create(self, daemon_id, host) -> str:
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'client.crash.' + host,
            'caps': ['mon', 'profile crash',
                     'mgr', 'profile crash'],
        })
        return self.mgr._create_daemon('crash', daemon_id, host, keyring=keyring)
