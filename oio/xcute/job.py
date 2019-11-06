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
import random
from datetime import datetime

from oio.common.easy_value import int_value, true_value
from oio.common.logger import get_logger


def uuid():
    return datetime.utcnow().strftime('%Y%m%d%H%M%S%f') \
        + '-%011X' % random.randrange(16**11)


class XcuteJob(object):
    """
    Dispatch tasks on the platform.
    """

    STATUS_WAITING = 'WAITING'
    STATUS_RUNNING = 'RUNNING'
    STATUS_PAUSED = 'PAUSED'
    STATUS_FINISHED = 'FINISHED'
    STATUS_FAILED = 'FAILED'

    DEFAULT_ITEMS_MAX_PER_SECOND = 30

    def __init__(self, conf, job_info, module_class,
                 create=False, logger=None):
        self.conf = conf
        self.job_info = job_info or dict()
        self.logger = logger or get_logger(self.conf)

        job_job = self.job_info.setdefault('job', dict())
        self.job_type = module_class.MODULE_TYPE
        if not self.job_type:
            raise ValueError('Missing job type')
        job_type = job_job.get('type')
        if job_type:
            if job_type != self.job_type:
                raise ValueError('Mismatch job type')
        else:
            job_type = job_job['type'] = self.job_type
        if create:
            self.job_status = self.STATUS_WAITING
            job_job['status'] = self.job_status
            self.job_id = uuid()
            job_job['id'] = self.job_id
            self.job_sending = True
            job_job['sending'] = str(self.job_sending)
        else:
            self.job_status = job_job.get('status')
            if not self.job_status:
                raise ValueError('Missing job status')
            self.job_id = job_job.get('id')
            if not self.job_id:
                raise ValueError('Missing job ID')
            self.job_sending = job_job.get('sending')
            if self.job_sending is None:
                raise ValueError('Missing job sending')
            self.job_sending = true_value(self.job_sending)

        job_items = self.job_info.setdefault('items', dict())
        self.items_max_per_second = int_value(
            job_items.get('max_per_second'),
            self.DEFAULT_ITEMS_MAX_PER_SECOND)
        job_items['max_per_second'] = self.items_max_per_second
        self.items_sent = int_value(job_items.get('sent'), 0)
        job_items['sent'] = self.items_sent
        self.items_last_sent = job_items.get('last_sent')
        self.items_processed = int_value(job_items.get('processed'), 0)
        job_items['processed'] = self.items_processed
        self.items_expected = int_value(job_items.get('expected'), None)
        job_items['expected'] = self.items_expected

        job_errors = self.job_info.setdefault('errors', dict())
        self.errors_total = int_value(job_errors.get('total'), 0)
        job_errors['total'] = self.errors_total
        self.errors_details = dict()
        for key, value in job_errors.items():
            if key == 'total':
                continue
            self.errors_details[key] = int_value(value, 0)
            job_errors[key] = self.errors_details[key]

        job_options = self.job_info.setdefault('options', dict())
        self.module = module_class(self.conf, job_options)
        self.job_lock = self.module.lock
        job_job['lock'] = self.job_lock

    def get_tasks_with_args(self):
        return self.module.get_tasks_with_args(last_item=self.items_last_sent)

    def send_task(self, item):
        self.items_sent += 1
        self.items_last_sent = item
        job_info = {
            'items': {
                'sent': self.items_sent,
                'last_sent': self.items_last_sent
            }
        }

        details = self.module.send_task(item)
        if details:
            job_info['details'] = details

        return job_info

    def receive_reply(self, reply_info):
        job_info = dict()

        self.items_processed += 1
        job_info['items'] = {
            'processed': self.items_processed
        }

        exc = pickle.loads(reply_info['exc'])
        if exc:
            self.logger.warn('Job %s (%s): %s',
                             self.job_id, self.job_type, exc)
            self.errors_total += 1
            exc_name = exc.__class__.__name__
            exc_nb = self.errors_details.get(exc_name, 0) + 1
            self.errors_details[exc_name] = exc_nb
            job_info['errors'] = {
                'total': self.errors_total,
                exc_name: exc_nb
            }
        else:
            res = pickle.loads(reply_info['res'])
            details = self.module.receive_result(res)
            if details:
                job_info['details'] = details

        return job_info

    def all_tasks_sent(self):
        self.job_sending = False
        job_info = {
            'job': {
                'sending': str(self.job_sending)
            }
        }
        return job_info

    def is_finished(self):
        """
        Tell if all workers have finished to process their tasks.
        """
        if self.job_sending:
            return False

        return self.items_processed >= self.items_sent
