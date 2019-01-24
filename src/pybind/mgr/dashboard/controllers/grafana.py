# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
import requests
import os

from . import ApiController, BaseController, Endpoint, ReadPermission, UpdatePermission
from .. import logger
from ..security import Scope
from ..settings import Settings


class GrafanaError(Exception):
    pass


class GrafanaRestClient(object):

    def url_validation(self, method, path):
        response = requests.request(
            method,
            path)

        return response.status_code

    def push_dashboard(self, dashboard_obj):
        if not Settings.GRAFANA_API_URL:
            raise GrafanaError("The Grafana API URL is not set!")
        if not Settings.GRAFANA_API_URL.startswith('http'):
            raise GrafanaError("The Grafana API URL is invalid")
        if not Settings.GRAFANA_API_USERNAME:
            raise GrafanaError("The Grafana API username is not set!")
        if not Settings.GRAFANA_API_PASSWORD:
            raise GrafanaError("The Grafana API password is not set!")
        url = Settings.GRAFANA_API_URL.rstrip('/') + \
            '/api/dashboards/db'
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        payload = {
            'dashboard': dashboard_obj,
            'overwrite': True,
        }
        response = requests.post(
            url,
            headers=headers,
            data=json.dumps(payload),
            auth=(Settings.GRAFANA_API_USERNAME,
                  Settings.GRAFANA_API_PASSWORD),
        )
        response.raise_for_status()
        return response.status_code, response.json()


def load_local_dashboards(path='../grafana_dashboards'):
    if not path.startswith(os.path.sep):
        path = os.path.join(os.path.dirname(__file__), path)
    path = os.path.abspath(path)
    dashboards = dict()
    for item in filter(lambda s: s.endswith('.json'), os.listdir(path)):
        db_path = os.path.join(path, item)
        dashboards[item] = json.loads(open(db_path).read())
    return dashboards


def push_local_dashboards():
    try:
        dashboards = load_local_dashboards()
    except Exception:
        logger.exception("Failed to load local dashboard files")
        raise
    try:
        grafana = GrafanaRestClient()
        for name, body in dashboards.items():
            grafana.push_dashboard(body)
    except Exception:
        logger.exception("Failed to push dashboards to Grafana")
        raise
    return True


@ApiController('/grafana', Scope.GRAFANA)
class Grafana(BaseController):

    @Endpoint()
    @ReadPermission
    def url(self):
        response = {'instance': Settings.GRAFANA_API_URL}
        return response

    @Endpoint()
    @ReadPermission
    def validation(self, params):
        grafana = GrafanaRestClient()
        method = 'GET'
        url = Settings.GRAFANA_API_URL.rstrip('/') + \
            '/api/dashboards/uid/' + params
        response = grafana.url_validation(method, url)
        return response

    @Endpoint(method='POST')
    @UpdatePermission
    def update_dashboards(self):
        response = dict()
        try:
            response['success'] = push_local_dashboards()
        except Exception as e:
            response['error'] = e.message
            response['success'] = False
        return response
