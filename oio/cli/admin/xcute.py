# Copyright (C) 2019 OpenIO SAS, as part of OpenIO SDS
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from datetime import datetime

from oio.cli import Command, Lister, ShowOne
from oio.xcute.client import XcuteClient


def _flat_dict_from_dict(parsed_args, dict_):
    """
    Create a dictionary without depth.

    {
        'depth0': {
            'depth1': {
                'depth2': 'test'
            }
        }
    }
    =>
    {
        'depth0.depth1.depth2': 'test'
    }
    """
    flat_dict = dict()
    for key, value in dict_.items():
        if not isinstance(value, dict):
            if isinstance(value, list) and parsed_args.formatter == 'table':
                value = '\n'.join(value)
            flat_dict[key] = value
            continue

        _flat_dict = _flat_dict_from_dict(parsed_args, value)
        for _key, _value in _flat_dict.items():
            flat_dict[key + '.' + _key] = _value
    return flat_dict


class XcuteCommand(object):

    _client = None

    @property
    def logger(self):
        return self.app.client_manager.logger

    @property
    def client(self):
        if self._client is None:
            self._client = XcuteClient(
                self.app.client_manager.client_conf, logger=self.logger)
        return self._client


class XcuteJobList(XcuteCommand, Lister):
    """
    List all jobs
    """

    columns = ('ID', 'Status', 'Type', 'Lock', 'ctime', 'mtime')

    def _take_action(self, parsed_args):
        jobs = self.client.job_list()
        for job_info in jobs:
            job_job = job_info['job']
            job_time = job_info['time']
            yield (job_job['id'], job_job['status'], job_job['type'],
                   job_job.get('lock', ''),
                   datetime.utcfromtimestamp(float(job_time['ctime'])),
                   datetime.utcfromtimestamp(float(job_time['mtime'])))

    def take_action(self, parsed_args):
        self.logger.debug('take_action(%s)', parsed_args)

        return self.columns, self._take_action(parsed_args)


class XcuteJobWaiting(XcuteCommand, Lister):
    """
    List all waiting jobs
    """

    columns = ('ID', 'Status', 'Type', 'Lock', 'ctime', 'mtime')

    def _take_action(self, parsed_args):
        jobs = self.client.job_waiting()
        for job_info in jobs:
            job_job = job_info['job']
            job_time = job_info['time']
            yield (job_job['id'], job_job['status'], job_job['type'],
                   job_job.get('lock', ''),
                   datetime.utcfromtimestamp(float(job_time['ctime'])),
                   datetime.utcfromtimestamp(float(job_time['mtime'])))

    def take_action(self, parsed_args):
        self.logger.debug('take_action(%s)', parsed_args)

        return self.columns, self._take_action(parsed_args)


class XcuteJobShow(XcuteCommand, ShowOne):
    """
    Get all informations about the job
    """

    def get_parser(self, prog_name):
        parser = super(XcuteJobShow, self).get_parser(prog_name)
        parser.add_argument(
            'job_id',
            metavar='<job_id>',
            help=("Job ID to show"))
        return parser

    def take_action(self, parsed_args):
        self.logger.debug('take_action(%s)', parsed_args)

        job_info = self.client.job_show(parsed_args.job_id)
        if parsed_args.formatter == 'table':
            job_time = job_info['time']
            job_time['ctime'] = datetime.utcfromtimestamp(
                float(job_time['ctime']))
            job_time['mtime'] = datetime.utcfromtimestamp(
                float(job_time['mtime']))
        return zip(*sorted(
            _flat_dict_from_dict(parsed_args, job_info).items()))


class XcuteJobPause(XcuteCommand, Command):
    """
    Pause the jobs
    """

    columns = ('ID', 'Paused')

    def get_parser(self, prog_name):
        parser = super(XcuteJobPause, self).get_parser(prog_name)
        parser.add_argument(
            'job_ids',
            nargs='+',
            metavar='<job_id>',
            help=("Job IDs to pause"))
        return parser

    def _take_action(self, parsed_args):
        for job_id in parsed_args.job_ids:
            paused = True
            try:
                self.client.job_resume(job_id)
            except Exception as exc:
                self.logger.error('Failed to paused job %s: %s',
                                  job_id, exc)
                paused = False
            yield (job_id, paused)

    def take_action(self, parsed_args):
        self.logger.debug('take_action(%s)', parsed_args)

        return self.columns, self._take_action(parsed_args)


class XcuteJobResume(XcuteCommand, Command):
    """
    Resume the jobs
    """

    columns = ('ID', 'Resumed')

    def get_parser(self, prog_name):
        parser = super(XcuteJobResume, self).get_parser(prog_name)
        parser.add_argument(
            'job_ids',
            nargs='+',
            metavar='<job_id>',
            help=("Job IDs to resume"))
        return parser

    def _take_action(self, parsed_args):
        for job_id in parsed_args.job_ids:
            resumed = True
            try:
                self.client.job_resume(job_id)
            except Exception as exc:
                self.logger.error('Failed to resumed job %s: %s',
                                  job_id, exc)
                resumed = False
            yield (job_id, resumed)

    def take_action(self, parsed_args):
        self.logger.debug('take_action(%s)', parsed_args)

        return self.columns, self._take_action(parsed_args)


class XcuteJobDelete(XcuteCommand, Lister):
    """
    Delete all informations about the jobs
    """

    columns = ('ID', 'Deleted')

    def get_parser(self, prog_name):
        parser = super(XcuteJobDelete, self).get_parser(prog_name)
        parser.add_argument(
            'job_ids',
            nargs='+',
            metavar='<job_id>',
            help=("Job IDs to delete"))
        return parser

    def _take_action(self, parsed_args):
        for job_id in parsed_args.job_ids:
            deleted = True
            try:
                self.client.job_delete(job_id)
            except Exception as exc:
                self.logger.error('Failed to deleted job %s: %s',
                                  job_id, exc)
                deleted = False
            yield (job_id, deleted)

    def take_action(self, parsed_args):
        self.logger.debug('take_action(%s)', parsed_args)

        return self.columns, self._take_action(parsed_args)


class XcuteJobLocks(XcuteCommand, Lister):
    """
    Get all the locks used.
    """

    columns = ('Key', 'Owner')

    def _take_action(self, parsed_args):
        for key, owner in self.client.locks().items():
            yield (key, owner)

    def take_action(self, parsed_args):
        self.logger.debug('take_action(%s)', parsed_args)

        return self.columns, self._take_action(parsed_args)


class XcuteOrchestratorJobs(XcuteCommand, Lister):
    """
    Get all jobs for the orchestrator.
    """

    columns = ('ID', 'Status', 'Type', 'Lock', 'ctime', 'mtime')

    def get_parser(self, prog_name):
        parser = super(XcuteOrchestratorJobs, self).get_parser(prog_name)
        parser.add_argument(
            'orchestrator_id',
            metavar='<orchestrator_id>',
            help=("Orchestrator ID to list"))
        return parser

    def _take_action(self, parsed_args):
        jobs = self.client.orchestrator_jobs(parsed_args.orchestrator_id)
        for job_info in jobs:
            job_job = job_info['job']
            job_time = job_info['time']
            yield (job_job['id'], job_job['status'], job_job['type'],
                   job_job.get('lock', ''),
                   datetime.utcfromtimestamp(float(job_time['ctime'])),
                   datetime.utcfromtimestamp(float(job_time['mtime'])))

    def take_action(self, parsed_args):
        self.logger.debug('take_action(%s)', parsed_args)

        return self.columns, self._take_action(parsed_args)
