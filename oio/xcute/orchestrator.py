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

import os
import pickle
import socket

from oio.common.exceptions import ExplicitBury, OioException, OioTimeout
from oio.common.logger import get_logger
from oio.common.green import ratelimit, sleep, thread, threading
from oio.common.json import json
from oio.conscience.client import ConscienceClient
from oio.event.beanstalk import Beanstalk, BeanstalkdListener, BeanstalkdSender
from oio.xcute.backend import XcuteBackend
from oio.xcute.job import XcuteJob
from oio.xcute.modules import get_module_class


class XcuteOrchestrator(object):

    def __init__(self, conf, verbose):
        self.conf = conf
        self.logger = get_logger(self.conf, verbose=verbose)
        self.backend = XcuteBackend(self.conf, logger=self.logger)
        self.conscience_client = ConscienceClient(self.conf)
        self.threads = dict()
        self.jobs = dict()

        # Orchestrator info
        self.orchestrator_id = \
            self.conf.get('orchestrator_id', socket.gethostname())
        self.logger.info('Using orchestrator id %s' % self.orchestrator_id)

        # Beanstalkd reply
        beanstalkd_reply_addr = self.conf.get('beanstalkd_reply_addr')
        if not beanstalkd_reply_addr:
            raise ValueError('Missing beanstalkd reply address')
        beanstalkd_reply_tube = self.conf['beanstalkd_reply_tube']
        if not beanstalkd_reply_tube:
            raise ValueError('Missing beanstalkd reply tube')
        self.beanstalkd_reply = BeanstalkdListener(
            beanstalkd_reply_addr, beanstalkd_reply_tube, self.logger)
        self.logger.info(
            'Beanstalkd %s using tube %s is used for the replies',
            self.beanstalkd_reply.addr, self.beanstalkd_reply.tube)

        # Prepare beanstalkd workers
        self.beanstalkd_workers_tube = self.conf['beanstalkd_workers_tube']
        if not self.beanstalkd_workers_tube:
            raise ValueError('Missing beanstalkd workers tube')
        self.beanstalkd_workers = list()

        self.running = True

    def _locate_tube(self, services, tube):
        """
        Get a list of beanstalkd services hosting the specified tube.

        :param services: known beanstalkd services.
        :type services: iterable of dictionaries
        :param tube: the tube to locate.
        :returns: a list of beanstalkd services hosting the the specified tube.
        :rtype: `list` of `dict`
        """
        available = list()
        for bsd in services:
            tubes = Beanstalk.from_url(
                'beanstalk://' + bsd['addr']).tubes()
            if tube in tubes:
                available.append(bsd)
        return available

    def _get_available_beanstalkd_workers(self):
        """
        Get available beanstalkd workers.
        """

        # Get all available beanstalkd
        all_beanstalkd = self.conscience_client.all_services('beanstalkd')
        all_available_beanstalkd = dict()
        for beanstalkd in all_beanstalkd:
            if beanstalkd['score'] <= 0:
                continue
            all_available_beanstalkd[beanstalkd['addr']] = beanstalkd
        if not all_available_beanstalkd:
            raise OioException('No beanstalkd available')

        # Get beanstalk workers
        beanstalkd_workers = list()
        for beanstalkd in self._locate_tube(all_available_beanstalkd.values(),
                                            self.beanstalkd_workers_tube):
            beanstalkd_worker = BeanstalkdSender(
                beanstalkd['addr'], self.beanstalkd_workers_tube, self.logger)
            beanstalkd_workers.append(beanstalkd_worker)
            self.logger.info(
                'Beanstalkd %s using tube %s is used as a worker',
                beanstalkd_worker.addr, beanstalkd_worker.tube)
        if not beanstalkd_workers:
            raise OioException('No beanstalkd worker available')
        return beanstalkd_workers

    def refresh_beanstalkd_workers(self):
        """
        Get all the beanstalkd and their tubes
        """

        while self.running:
            try:
                self.beanstalkd_workers = \
                    self._get_available_beanstalkd_workers()
            except Exception as exc:
                self.logger.error(
                    'Failed to search for beanstalkd workers: %s', exc)
            sleep(5)

        self.logger.info('Exited beanstalkd thread')

    def _receive_reply(self, beanstalkd_job_id, beanstalkd_job_data, **kwargs):
        reply_info = json.loads(beanstalkd_job_data)

        job_id = reply_info.get('job_id')
        if not job_id:
            raise ExplicitBury('No job ID')
        job = self.jobs.get(job_id)
        if not job:
            raise ExplicitBury('Unknown job ID')

        job_info = job.receive_reply(reply_info)
        self.backend.update_job(job_id, job_info)
        if job.is_finished():
            self.backend.finish_job(job_id)

        yield None

    def receive_replies(self):
        """
        Process this orchestrator's job replies
        """

        try:
            while self.running:
                try:
                    replies = self.beanstalkd_reply.fetch_job(
                        self._receive_reply, timeout=1)
                    for _ in replies:
                        pass
                except OioTimeout:
                    pass
        except Exception as exc:
            self.logger.error('Failed to fetch task results: %s', exc)
            self.exit()

        self.logger.info('Exited listening thread')

    def start_new_jobs(self):
        """
        One iteration of the main loop
        """
        while self.running:
            job_info = self.backend.start_new_job(self.orchestrator_id)
            if not job_info:
                sleep(5)
                continue

            self.logger.info(job_info)
            self.logger.info('Found new job %s (%s)',
                             job_info['job']['id'], job_info['job']['type'])
            self.handle_job(job_info)

        self.logger.debug('Finished orchestrating loop')

    def handle_job(self, job_info):
        """
        Get the beanstalkd available for this job
        and start the dispatching thread
        """
        try:
            module_class = get_module_class(job_info)
            job = XcuteJob(self.conf, job_info, module_class,
                           logger=self.logger)

            self.jobs[job.job_id] = job
            if job.job_status == XcuteJob.STATUS_RUNNING:
                dispatch_tasks_thread = threading.Thread(
                    target=self.dispatch_tasks, args=(job, ))
                dispatch_tasks_thread.start()
                self.threads[dispatch_tasks_thread.ident] = \
                    dispatch_tasks_thread
        except Exception as exc:
            self.logger.error(
                'Failed to instantiate job %s (%s): %s',
                job_info['job']['id'], job_info['job']['type'], exc)
            self.backend.fail_job(job_info['job']['id'])

    def _beanstalkd_job_data_from_task(self, job, task_class, task_item,
                                       task_kwargs):
        beanstalkd_job = dict()
        beanstalkd_job['job_id'] = job.job_id
        beanstalkd_job['task'] = pickle.dumps(task_class)
        beanstalkd_job['item'] = task_item
        beanstalkd_job['kwargs'] = task_kwargs or dict()
        beanstalkd_job['beanstalkd_reply'] = {
            'addr': self.beanstalkd_reply.addr,
            'tube': self.beanstalkd_reply.tube}
        return json.dumps(beanstalkd_job)

    def _dispatch_task(self, job, task_with_args, next_worker):
        """
        Send the task through a non-full sender.
        """
        _, item, _ = task_with_args
        beanstalkd_job_data = self._beanstalkd_job_data_from_task(
            job, *task_with_args)
        while self.running:
            workers = self.beanstalkd_workers
            nb_workers = len(workers)
            for _ in range(nb_workers):
                next_worker = next_worker % nb_workers
                success = workers[next_worker].send_job(beanstalkd_job_data)
                workers[next_worker].job_done()
                next_worker = next_worker + 1
                if success:
                    job_info = job.send_task(item)
                    self.backend.update_job(job.job_id, job_info)
                    return next_worker
            self.logger.warn('All beanstalkd workers are full')
            sleep(5)

    def dispatch_tasks(self, job):
        next_worker = 0
        items_run_time = 0

        try:
            tasks_with_args = job.get_tasks_with_args()
            items_run_time = ratelimit(
                items_run_time, job.items_max_per_second)
            next_worker = self._dispatch_task(
                job, next(tasks_with_args), next_worker)
            for task_with_args in tasks_with_args:
                items_run_time = ratelimit(items_run_time,
                                           job.items_max_per_second)
                next_worker = self._dispatch_task(
                    job, task_with_args, next_worker)

                if not self.running:
                    return
        except Exception as exc:
            if not isinstance(exc, StopIteration):
                self.logger.error(
                    'Failed to dispatch tasks for job %s (%s): %s',
                    job.job_id, job.job_type, exc)
                self.backend.fail_job(job.job_id)
                return

        self.logger.info('All tasks sent for job %s (%s)',
                         job.job_id, job.job_type)
        job_info = job.all_tasks_sent()
        self.backend.update_job(job.job_id, job_info)

        # threading.current_thread returns the wrong id
        del self.threads[thread.get_ident()]

    def run_forever(self):
        """
        Take jobs from the queue and spawn threads to dispatch them
        """

        # gather beanstalkd info
        refresh_beanstalkd_workers_thread = threading.Thread(
            target=self.refresh_beanstalkd_workers)
        refresh_beanstalkd_workers_thread.start()
        self.threads[refresh_beanstalkd_workers_thread.ident] = \
            refresh_beanstalkd_workers_thread

        self.logger.info('Wait until beanstalkd workers are found')
        while len(self.beanstalkd_workers) == 0:
            sleep(5)

        # restart running jobs
        self.logger.debug('Look for unfinished jobs')
        orchestrator_jobs = \
            self.backend.list_orchestrator_jobs(self.orchestrator_id)
        for job_info in orchestrator_jobs:
            self.logger.info('Found running job %s (%s)',
                             job_info['job']['id'], job_info['job']['type'])
            self.handle_job(job_info)

        # start processing replies
        receive_replies_thread = threading.Thread(
            target=self.receive_replies)
        receive_replies_thread.start()
        self.threads[receive_replies_thread.ident] = \
            receive_replies_thread

        # start new jobs
        self.start_new_jobs()

        # exiting
        for thread_ in self.threads.values():
            thread_.join()

    def exit_gracefully(self, *args, **kwargs):
        if self.running:
            self.logger.info('Exiting gracefully')
            self.running = False
            return

        self.logger.info('Exiting immediately')
        os._exit(1)
