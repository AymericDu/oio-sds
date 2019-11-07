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

import pickle

from oio.common.green import sleep
from oio.common.json import json
from oio.common.logger import get_logger
from oio.common.utils import request_id
from oio.event.beanstalk import BeanstalkdSender
from oio.xcute.task import XcuteTask


class XcuteWorker(object):

    def __init__(self, conf, logger=None):
        self.beanstalkd_reply = None
        self.conf = conf
        self.logger = logger or get_logger(self.conf)

    def _reply(self, beanstalkd_job, res, exc):
        reply_dest = beanstalkd_job.get('beanstalkd_reply')
        if not reply_dest:
            return

        beanstalkd_job['res'] = pickle.dumps(res)
        beanstalkd_job['exc'] = pickle.dumps(exc)
        beanstalkd_job_data = json.dumps(beanstalkd_job)

        try:
            if self.beanstalkd_reply is None \
                    or self.beanstalkd_reply.addr != reply_dest['addr'] \
                    or self.beanstalkd_reply.tube != reply_dest['tube']:
                if self.beanstalkd_reply is not None:
                    self.beanstalkd_reply.close()
                self.beanstalkd_reply = BeanstalkdSender(
                    reply_dest['addr'], reply_dest['tube'], self.logger)

            sent = False
            while not sent:
                sent = self.beanstalkd_reply.send_job(beanstalkd_job_data)
                if not sent:
                    sleep(1.0)
            self.beanstalkd_reply.job_done()
        except Exception as exc:
            self.logger.warn('Fail to reply %s: %s', str(beanstalkd_job), exc)

    def process_beanstalkd_job(self, beanstalkd_job):
        try:
            # Decode the beanstakd job
            job_id = beanstalkd_job['job_id']
            task_class_encoded = beanstalkd_job['task']

            task_class = pickle.loads(task_class_encoded)
            task = task_class(self.conf, self.logger)

            if not isinstance(task, XcuteTask):
                raise ValueError('Unexpected task: %s' % task_class)

            task_item = beanstalkd_job['item']
            task_kwargs = beanstalkd_job.get('kwargs', dict())

            # Execute the task
            date, rand = job_id.split('-')
            reqid = date + '-' + request_id(prefix=rand+'-')
            res = task.process(task_item, reqid=reqid, **task_kwargs)
            exc = None
        except Exception as exc:
            self.logger.exception(exc)
            res = None

        if exc:
            self.logger.error('Error to process job %s: %s',
                              str(beanstalkd_job), exc)

        # Reply
        self._reply(beanstalkd_job, res, exc)
