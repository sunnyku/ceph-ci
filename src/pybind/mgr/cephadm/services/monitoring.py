import logging
import os
from typing import List, Any, Tuple, Dict

from cephadm.services.cephadmservice import CephadmService
from mgr_util import verify_tls, ServerConfigException, create_self_signed_cert

logger = logging.getLogger(__name__)

class GrafanaService(CephadmService):
    def create(self, daemon_id, host):
        # type: (str, str) -> str
        return self.mgr._create_daemon('grafana', daemon_id, host)

    def generate_config(self):
        # type: () -> Tuple[Dict[str, Any], List[str]]
        deps = []  # type: List[str]
        def generate_grafana_ds_config(hosts: List[str]) -> str:
            config = '''# generated by cephadm
deleteDatasources:
{delete_data_sources}

datasources:
{data_sources}
'''
            delete_ds_template = '''
  - name: '{name}'
    orgId: 1\n'''.lstrip('\n')
            ds_template = '''
  - name: '{name}'
    type: 'prometheus'
    access: 'proxy'
    orgId: 1
    url: 'http://{host}:9095'
    basicAuth: false
    isDefault: {is_default}
    editable: false\n'''.lstrip('\n')

            delete_data_sources = ''
            data_sources = ''
            for i, host in enumerate(hosts):
                name = "Dashboard %d" % (i + 1)
                data_sources += ds_template.format(
                    name=name,
                    host=host,
                    is_default=str(i == 0).lower()
                )
                delete_data_sources += delete_ds_template.format(
                    name=name
                )
            return config.format(
                delete_data_sources=delete_data_sources,
                data_sources=data_sources,
            )

        prom_services = []  # type: List[str]
        for dd in self.mgr.cache.get_daemons_by_service('prometheus'):
            prom_services.append(dd.hostname)
            deps.append(dd.name())

        cert = self.mgr.get_store('grafana_crt')
        pkey = self.mgr.get_store('grafana_key')
        if cert and pkey:
            try:
                verify_tls(cert, pkey)
            except ServerConfigException as e:
                logger.warning('Provided grafana TLS certificates invalid: %s', str(e))
                cert, pkey = None, None
        if not (cert and pkey):
            cert, pkey = create_self_signed_cert('Ceph', 'cephadm')
            self.mgr.set_store('grafana_crt', cert)
            self.mgr.set_store('grafana_key', pkey)
            self.mgr.check_mon_command({
                'prefix': 'dashboard set-grafana-api-ssl-verify',
                'value': 'false',
            })



        config_file = {
            'files': {
                "grafana.ini": """# generated by cephadm
[users]
  default_theme = light
[auth.anonymous]
  enabled = true
  org_name = 'Main Org.'
  org_role = 'Viewer'
[server]
  domain = 'bootstrap.storage.lab'
  protocol = https
  cert_file = /etc/grafana/certs/cert_file
  cert_key = /etc/grafana/certs/cert_key
  http_port = 3000
[security]
  admin_user = admin
  admin_password = admin
  allow_embedding = true
""",
                'provisioning/datasources/ceph-dashboard.yml': generate_grafana_ds_config(prom_services),
                'certs/cert_file': '# generated by cephadm\n%s' % cert,
                'certs/cert_key': '# generated by cephadm\n%s' % pkey,
            }
        }
        return config_file, sorted(deps)


class AlertmanagerService(CephadmService):
    def create(self, daemon_id, host) -> str:
        return self.mgr._create_daemon('alertmanager', daemon_id, host)

    def generate_config(self):
        # type: () -> Tuple[Dict[str, Any], List[str]]
        deps = [] # type: List[str]

        # dashboard(s)
        dashboard_urls = []
        mgr_map = self.mgr.get('mgr_map')
        port = None
        proto = None  # http: or https:
        url = mgr_map.get('services', {}).get('dashboard', None)
        if url:
            dashboard_urls.append(url)
            proto = url.split('/')[0]
            port = url.split('/')[2].split(':')[1]
        # scan all mgrs to generate deps and to get standbys too.
        # assume that they are all on the same port as the active mgr.
        for dd in self.mgr.cache.get_daemons_by_service('mgr'):
            # we consider mgr a dep even if the dashboard is disabled
            # in order to be consistent with _calc_daemon_deps().
            deps.append(dd.name())
            if not port:
                continue
            if dd.daemon_id == self.mgr.get_mgr_id():
                continue
            addr = self.mgr.inventory.get_addr(dd.hostname)
            dashboard_urls.append('%s//%s:%s/' % (proto, addr.split(':')[0],
                                                 port))

        yml = """# generated by cephadm
# See https://prometheus.io/docs/alerting/configuration/ for documentation.

global:
  resolve_timeout: 5m

route:
  group_by: ['alertname']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 1h
  receiver: 'ceph-dashboard'
receivers:
- name: 'ceph-dashboard'
  webhook_configs:
{urls}
""".format(
    urls='\n'.join(
        ["  - url: '{}api/prometheus_receiver'".format(u)
         for u in dashboard_urls]
    ))
        peers = []
        port = '9094'
        for dd in self.mgr.cache.get_daemons_by_service('alertmanager'):
            deps.append(dd.name())
            addr = self.mgr.inventory.get_addr(dd.hostname)
            peers.append(addr.split(':')[0] + ':' + port)
        return {
            "files": {
                "alertmanager.yml": yml
            },
            "peers": peers
        }, sorted(deps)


class PrometheusService(CephadmService):
    def create(self, daemon_id, host) -> str:
        return self.mgr._create_daemon('prometheus', daemon_id, host)

    def generate_config(self):
        # type: () -> Tuple[Dict[str, Any], List[str]]
        deps = []  # type: List[str]

        # scrape mgrs
        mgr_scrape_list = []
        mgr_map = self.mgr.get('mgr_map')
        port = None
        t = mgr_map.get('services', {}).get('prometheus', None)
        if t:
            t = t.split('/')[2]
            mgr_scrape_list.append(t)
            port = '9283'
            if ':' in t:
                port = t.split(':')[1]
        # scan all mgrs to generate deps and to get standbys too.
        # assume that they are all on the same port as the active mgr.
        for dd in self.mgr.cache.get_daemons_by_service('mgr'):
            # we consider the mgr a dep even if the prometheus module is
            # disabled in order to be consistent with _calc_daemon_deps().
            deps.append(dd.name())
            if not port:
                continue
            if dd.daemon_id == self.mgr.get_mgr_id():
                continue
            addr = self.mgr.inventory.get_addr(dd.hostname)
            mgr_scrape_list.append(addr.split(':')[0] + ':' + port)

        # scrape node exporters
        node_configs = ''
        for dd in self.mgr.cache.get_daemons_by_service('node-exporter'):
            deps.append(dd.name())
            addr = self.mgr.inventory.get_addr(dd.hostname)
            if not node_configs:
                node_configs = """
  - job_name: 'node'
    static_configs:
"""
            node_configs += """    - targets: {}
      labels:
        instance: '{}'
""".format([addr.split(':')[0] + ':9100'],
           dd.hostname)

        # scrape alert managers
        alertmgr_configs = ""
        alertmgr_targets = []
        for dd in self.mgr.cache.get_daemons_by_service('alertmanager'):
            deps.append(dd.name())
            addr = self.mgr.inventory.get_addr(dd.hostname)
            alertmgr_targets.append("'{}:9093'".format(addr.split(':')[0]))
        if alertmgr_targets:
            alertmgr_configs = """alerting:
  alertmanagers:
    - scheme: http
      path_prefix: /alertmanager
      static_configs:
        - targets: [{}]
""".format(", ".join(alertmgr_targets))

        # generate the prometheus configuration
        r = {
            'files': {
                'prometheus.yml': """# generated by cephadm
global:
  scrape_interval: 5s
  evaluation_interval: 10s
rule_files:
  - /etc/prometheus/alerting/*
{alertmgr_configs}
scrape_configs:
  - job_name: 'ceph'
    static_configs:
    - targets: {mgr_scrape_list}
      labels:
        instance: 'ceph_cluster'
{node_configs}
""".format(
    mgr_scrape_list=str(mgr_scrape_list),
    node_configs=str(node_configs),
    alertmgr_configs=str(alertmgr_configs)
    ),
            },
        }

        # include alerts, if present in the container
        if os.path.exists(self.mgr.prometheus_alerts_path):
            with open(self.mgr.prometheus_alerts_path, 'r', encoding='utf-8') as f:
                alerts = f.read()
            r['files']['/etc/prometheus/alerting/ceph_alerts.yml'] = alerts

        return r, sorted(deps)


class NodeExporterService(CephadmService):
    def create(self, daemon_id, host) -> str:
        return self.mgr._create_daemon('node-exporter', daemon_id, host)

    def generate_config(self) -> Tuple[Dict[str, Any], List[str]]:
        return {}, []
