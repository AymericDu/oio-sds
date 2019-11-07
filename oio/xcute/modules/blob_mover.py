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

from oio.blob.client import BlobClient
from oio.common.easy_value import float_value, int_value
from oio.common.exceptions import ContentNotFound, OrphanChunk
from oio.conscience.client import ConscienceClient
from oio.content.factory import ContentFactory
from oio.rdir.client import RdirClient
from oio.xcute.module import XcuteModule
from oio.xcute.task import XcuteTask


class BlobMover(XcuteTask):

    def __init__(self, conf, logger):
        super(BlobMover, self).__init__(conf, logger)
        self.blob_client = BlobClient(
            self.conf, logger=self.logger)
        self.content_factory = ContentFactory(conf)
        self.conscience_client = ConscienceClient(
            self.conf, logger=self.logger)

    def _generate_fake_excluded_chunks(self, excluded_rawx):
        fake_excluded_chunks = list()
        fake_chunk_id = '0'*64
        for service_id in excluded_rawx:
            service_addr = self.conscience_client.resolve_service_id(
                'rawx', service_id)
            chunk = dict()
            chunk['hash'] = '0000000000000000000000000000000000'
            chunk['pos'] = '0'
            chunk['size'] = 1
            chunk['score'] = 1
            chunk['url'] = 'http://' + service_id + '/' + fake_chunk_id
            chunk['real_url'] = 'http://' + service_addr + '/' + fake_chunk_id
            fake_excluded_chunks.append(chunk)
        return fake_excluded_chunks

    def process(self, chunk_id, rawx_id=None, rawx_timeout=None,
                min_chunk_size=None, max_chunk_size=None, excluded_rawx=None,
                **kwargs):
        if not rawx_id:
            raise ValueError('No rawx ID')
        min_chunk_size = min_chunk_size \
            or RawxDecommissionModule.DEFAULT_MIN_CHUNK_SIZE
        max_chunk_size = max_chunk_size \
            or RawxDecommissionModule.DEFAULT_MAX_CHUNK_SIZE
        excluded_rawx = excluded_rawx \
            or RawxDecommissionModule.DEFAULT_EXCLUDED_RAWX

        fake_excluded_chunks = self._generate_fake_excluded_chunks(
            excluded_rawx)

        chunk_url = '/'.join(('http:/', rawx_id, chunk_id))
        meta = self.blob_client.chunk_head(chunk_url, timeout=rawx_timeout,
                                           **kwargs)
        container_id = meta['container_id']
        content_id = meta['content_id']
        chunk_id = meta['chunk_id']

        # Maybe skip the chunk because it doesn't match the size constaint
        chunk_size = int(meta['chunk_size'])
        if chunk_size < min_chunk_size:
            self.logger.debug("SKIP %s too small", chunk_url)
            return
        if max_chunk_size > 0 and chunk_size > max_chunk_size:
            self.logger.debug("SKIP %s too big", chunk_url)
            return

        # Start moving the chunk
        try:
            content = self.content_factory.get(container_id, content_id,
                                               **kwargs)
        except ContentNotFound:
            raise OrphanChunk('Content not found')

        new_chunk = content.move_chunk(
            chunk_id, fake_excluded_chunks=fake_excluded_chunks, **kwargs)

        self.logger.info('Moved chunk %s to %s', chunk_url, new_chunk['url'])
        return chunk_size


class RawxDecommissionModule(XcuteModule):

    MODULE_TYPE = 'rawx-decommission'
    DEFAULT_RDIR_FETCH_LIMIT = 1000
    DEFAULT_RDIR_TIMEOUT = 60.0
    DEFAULT_RAWX_TIMEOUT = 60.0
    DEFAULT_MIN_CHUNK_SIZE = 0
    DEFAULT_MAX_CHUNK_SIZE = 0
    DEFAULT_EXCLUDED_RAWX = list()

    def __init__(self, conf, options, details, logger=None):
        super(RawxDecommissionModule, self).__init__(
            conf, options, details, logger=logger)

        self.rawx_id = options.get('rawx_id')
        if not self.rawx_id:
            raise ValueError('Missing rawx ID')

        self.rdir_fetch_limit = int_value(
            self.options.get('rdir_fetch_limit'),
            self.DEFAULT_RDIR_FETCH_LIMIT)
        self.options['rdir_fetch_limit'] = self.rdir_fetch_limit
        self.rdir_timeout = float_value(
            self.options.get('rdir_timeout'), self.DEFAULT_RDIR_TIMEOUT)
        self.options['rdir_timeout'] = self.rdir_timeout
        self.rawx_timeout = float_value(
            self.options.get('rawx_timeout'), self.DEFAULT_RAWX_TIMEOUT)
        self.options['rawx_timeout'] = self.rawx_timeout
        self.min_chunk_size = int_value(
            self.options.get('min_chunk_size'), self.DEFAULT_MIN_CHUNK_SIZE)
        self.options['min_chunk_size'] = self.min_chunk_size
        self.max_chunk_size = int_value(
            self.options.get('max_chunk_size'), self.DEFAULT_MAX_CHUNK_SIZE)
        self.options['max_chunk_size'] = self.max_chunk_size
        excluded_rawx = self.options.get('excluded_rawx') or ''
        self.options['excluded_rawx'] = excluded_rawx
        self.excluded_rawx = [rawx for rawx in excluded_rawx.split(',')
                              if rawx]

        self.rdir_client = RdirClient(self.conf, logger=self.logger)

        self.lock = 'rawx/%s' % self.rawx_id

        details_chunks = self.details.setdefault('chunks', dict())
        self.chunks_size = int_value(details_chunks.get('size'), 0)

    def get_tasks_with_args(self, last_chunk_id):
        chunks_info = self.rdir_client.chunk_fetch(
            self.rawx_id, limit=self.rdir_fetch_limit,
            timeout=self.rdir_timeout, start_after=last_chunk_id)

        kwargs = {'rawx_id': self.rawx_id,
                  'rawx_timeout': self.rawx_timeout,
                  'min_chunk_size': self.min_chunk_size,
                  'max_chunk_size': self.max_chunk_size,
                  'excluded_rawx': self.excluded_rawx}
        for _, _, chunk_id, _ in chunks_info:
            yield (BlobMover, chunk_id, kwargs)

    def send_task(self, chunk_id):
        pass

    def receive_result(self, chunk_size):
        self.chunks_size += chunk_size
