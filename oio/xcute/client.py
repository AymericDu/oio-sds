# Copyright (C) 2019 OpenIO SAS, as part of OpenIO SDS
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3.0 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library.

from oio.api.base import HttpApi
from oio.common.logger import get_logger


class XcuteClient(HttpApi):
    """Simple client API for the xcute service."""

    def __init__(self, conf, logger=None, **kwargs):
        super(XcuteClient, self).__init__(
            endpoint='http://127.0.0.1:8000/v1.0/xcute',
            service_type='xcute-service', **kwargs)
        self.conf = conf
        self.logger = logger or get_logger(self.conf)

    def job_list(self):
        _, data = self._request('GET', '/jobs')
        return data

    def job_waiting(self):
        _, data = self._request('GET', '/jobs/waiting')
        return data

    def job_show(self, job_id):
        _, data = self._request('GET', '/jobs/%s' % job_id)
        return data

    def job_pause(self, job_id):
        self._request('POST', '/jobs/%s/pause' % job_id)

    def job_resume(self, job_id):
        self._request('POST', '/jobs/%s/resume' % job_id)

    def job_delete(self, job_id):
        self._request('DELETE', '/jobs/%s' % job_id)

    def locks(self):
        _, data = self._request('GET', '/jobs/locks')
        return data

    def orchestrator_jobs(self, orchestrator_id):
        _, data = self._request(
            'GET', '/orchestrator/%s/jobs' % orchestrator_id)
        return data
