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

from oio.common.logger import get_logger


class XcuteModule(object):

    MODULE_TYPE = None

    def __init__(self, conf, options, logger=None):
        self.conf = conf
        self.options = options or dict()
        self.logger = logger or get_logger(self.conf)
        self.lock = None

    def get_tasks_with_args(self):
        raise NotImplementedError()

    def send_task(self, item):
        raise NotImplementedError()

    def receive_result(self, result):
        raise NotImplementedError()
