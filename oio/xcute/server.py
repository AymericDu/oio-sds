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

from werkzeug.exceptions import BadRequest as HTTPBadRequest
from werkzeug.exceptions import NotFound as HTTPNotFound
from werkzeug.routing import Map, Rule, Submount
from werkzeug.wrappers import Response

from oio.common.exceptions import NotFound
from oio.common.json import json
from oio.common.logger import get_logger
from oio.common.wsgi import WerkzeugApp
from oio.xcute.backend import XcuteBackend
from oio.xcute.job import XcuteJob
from oio.xcute.modules import get_module_class


class XcuteServer(WerkzeugApp):
    def __init__(self, conf, backend, logger=None):
        self.conf = conf
        self.backend = backend
        self.logger = logger or get_logger(self.conf)

        url_map = Map([
            Submount('/v1.0/xcute', [
                Rule('/jobs', endpoint='job_list',
                     methods=['POST', 'GET']),
                Rule('/jobs/waiting', endpoint='job_waiting',
                     methods=['GET']),
                Rule('/jobs/locks', endpoint='job_locks',
                     methods=['GET']),
                Rule('/jobs/<job_id>', endpoint='job',
                     methods=['GET', 'DELETE']),
                Rule('/jobs/<job_id>/pause', endpoint='job_pause',
                     methods=['POST']),
                Rule('/jobs/<job_id>/resume', endpoint='job_resume',
                     methods=['POST']),
                Rule('/orchestrator/<orchestrator_id>/jobs',
                     endpoint='orchestrator_jobs',
                     methods=['GET']),
            ])
        ])

        super(XcuteServer, self).__init__(url_map, logger)

    def on_job_list(self, req):
        if req.method == 'GET':
            limit = int(req.args.get('limit', '1000'))
            marker = req.args.get('marker', '')
            jobs = self.backend.list_jobs(limit=limit, marker=marker)

            return Response(json.dumps(jobs), mimetype='application/json')

        if req.method == 'POST':
            try:
                job_info = json.loads(req.data)

                module_class = get_module_class(job_info)
                job = XcuteJob(self.conf, job_info, module_class,
                               create=True, logger=self.logger)
                self.backend.create_job(job.job_id, job.job_info)
            except ValueError as e:
                return HTTPBadRequest(e.message)

            return Response(json.dumps(job.job_info), status=202)

    def on_job_waiting(self, req):
        waiting = self.backend.get_waiting_jobs()
        return Response(json.dumps(waiting), mimetype='application/json')

    def on_job_locks(self, req):
        locks = self.backend.get_locks()
        return Response(json.dumps(locks), mimetype='application/json')

    def on_job(self, req, job_id):
        if req.method == 'GET':
            try:
                job = self.backend.get_job_info(job_id)
            except NotFound as e:
                return HTTPNotFound(e.message)

            return Response(json.dumps(job), mimetype='application/json')

        if req.method == 'DELETE':
            self.backend.delete_job(job_id)

            return Response(status=204)

    def on_job_pause(self, req, job_id):
        return Response(status=501)
        # do something
        # return Response(status=204)

    def on_job_resume(self, req, job_id):
        return Response(status=501)
        # do something
        # return Response(status=204)

    def on_orchestrator_jobs(self, req, orchestrator_id):
        jobs = self.backend.list_orchestrator_jobs(orchestrator_id)
        return Response(json.dumps(jobs), mimetype='application/json')


def create_app(conf):
    logger = get_logger(conf)
    backend = XcuteBackend(conf, logger=logger)
    app = XcuteServer(conf, backend, logger=logger)

    return app
