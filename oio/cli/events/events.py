# Copyright (C) 2015-2020 OpenIO SAS, as part of OpenIO SDS
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

from logging import getLogger

from oio.cli import Lister, ShowOne


class StatsEvents(ShowOne):
    """Stats events"""

    log = getLogger(__name__ + '.StatsEvents')

    def get_parser(self, prog_name):
        parser = super(StatsEvents, self).get_parser(prog_name)
        parser.add_argument(
            '--tube',
            metavar='<tube>',
            help='Tube name')
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)', parsed_args)

        data = self.app.client_manager.event.stats(parsed_args.tube)
        return list(zip(*sorted(data.items())))


class EventsExhume(ShowOne):
    """
    Exhume (replay) events that have been buried.
    """

    log = getLogger(__name__ + '.EventsExhume')

    def get_parser(self, prog_name):
        parser = super(EventsExhume, self).get_parser(prog_name)
        parser.add_argument(
            '--tube',
            default="oio",
            help='Name of the tube to interact with (defaults to "oio")')
        parser.add_argument(
            '--limit',
            type=int,
            default=1000,
            help='Maximum number of events to exhume (defaults to 1000)')
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)', parsed_args)

        count = self.app.client_manager.event.exhume(parsed_args.limit,
                                                     parsed_args.tube)
        return [("Exhumed",), (count,)]


class ListTubes(Lister):
    """
    Get the list of all tubes known by the beanstalkd service
    behind the local event-agent.
    """

    def take_action(self, parsed_args):
        tubes = self.app.client_manager.event.list_tubes()
        print(tubes)
        return [("Tubes",), ((x, ) for x in tubes)]
