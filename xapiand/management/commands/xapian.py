"""

Start the Xapiand server.

"""
from __future__ import absolute_import, unicode_literals

from xapiand.bin import worker

from django.core.management import BaseCommand


class Command(BaseCommand):
    option_list = BaseCommand.option_list + worker.option_list
    help = worker.help

    def handle(self, *args, **options):
        worker.run(*args, **options)
