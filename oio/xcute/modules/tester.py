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

import random

import oio.common.exceptions as exc
from oio.common.easy_value import int_value
from oio.xcute.module import XcuteModule
from oio.xcute.task import XcuteTask


EXCEPTIONS = [exc.BadRequest,
              exc.Forbidden,
              exc.NotFound,
              exc.MethodNotAllowed,
              exc.Conflict,
              exc.ClientPreconditionFailed,
              exc.TooLarge,
              exc.UnsatisfiableRange,
              exc.ServiceBusy]


ITEMS = list()
for i in range(1000):
    ITEMS.append('myitem-' + str(i))


class TesterTask(XcuteTask):

    def process(self, item, error_percentage=None, **kwargs):
        if error_percentage and random.randrange(100) < error_percentage:
            exc_class = random.choice(EXCEPTIONS)
            raise exc_class()
        self.logger.error('It works (item=%s ; kwargs=%s) !!!',
                          item, str(kwargs))


class TesterModule(XcuteModule):

    MODULE_TYPE = 'tester'
    DEFAULT_ERROR_PERCENTAGE = 0

    def __init__(self, conf, options, details, logger=None):
        super(TesterModule, self).__init__(
            conf, options, details, logger=logger)
        self.lock = self.options.get('lock')

        self.error_percentage = int_value(
            self.options.get('error_percentage'),
            self.DEFAULT_ERROR_PERCENTAGE)
        self.options['error_percentage'] = self.error_percentage

    def get_tasks_with_args(self, last_item):
        start_index = 0
        if last_item is not None:
            start_index = ITEMS.index(last_item) + 1

        kwargs = {'lock': self.lock,
                  'error_percentage': self.error_percentage}
        for item in ITEMS[start_index:]:
            yield (TesterTask, item, kwargs)

    def send_task(self, item):
        pass

    def receive_result(self, result):
        pass
