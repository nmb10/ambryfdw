"""
Purpose
-------

This fdw can be used to access data stored in the ambry partitions packed with msgpack. Each column defined
in the table will be mapped, in order, against columns in the ambry partition file.

Dependencies
------------

No dependency outside the standard python distribution.

Options
----------------

``filename`` (required)
  The full path to the gzipped msg file containing the data. This file must be readable
  to the postgres user.

Usage example
-------------

Supposing you want to parse the following gzipped msgpack partition file, located in ``/tmp/test.msg``::

    Year,Make,Model,Length
    1997,Ford,E350,2.34
    2000,Mercury,Cougar,2.38

You can declare the following table:

.. code-block:: sql

    CREATE SERVER partition_srv foreign data wrapper multicorn options (
        wrapper 'ambryfdw.PartitionMsgpackForeignDataWrapper'
    );

    create foreign table partition_test (
           year numeric,
           make character varying,
           model character varying,
           length numeric
    ) server partition_srv options (
           filename '/tmp/test.msg');

    select * from partition_test;

.. code-block:: bash

     year |  make   | model  | length
    ------+---------+--------+--------
     1997 | Ford    | E350   |   2.34
     2000 | Mercury | Cougar |   2.38
    (2 lines)

"""

from gzip import GzipFile
from datetime import datetime, time
import operator
import re

import msgpack

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING

# Note:
#    date and time formats listed here have to match to formats used in the
#    ambry.etl.partition.PartitionMsgpackDataFileReader.decode_obj

DATETIME_FORMAT_NO_MS = '%Y-%m-%dT%H:%M:%S'
DATETIME_FORMAT_WITH_MS = '%Y-%m-%dT%H:%M:%S.%f'
TIME_FORMAT = '%H:%M:%S'
DATE_FORMAT = '%Y-%m-%d'


def like_op(a, b):
    """ Returns True if 'a LIKE b'. """
    # FIXME: Optimize
    r_exp = b.replace('%', '.*').replace('_', '.{1}') + '$'
    return bool(re.match(r_exp, a))


def ilike_op(a, b):
    """ Returns True if 'a ILIKE 'b. FIXME: is it really ILIKE? """
    return like_op(a.lower(), b.lower())


def not_like_op(a, b):
    """ Returns True if 'a NOT LIKE b'. FIXME: is it really NOT? """
    return not like_op(a, b)


def not_ilike_op(a, b):
    """ Returns True if 'a NOT LIKE b'. FIXME: is it really NOT? """
    return not ilike_op(a, b)


QUAL_OPERATOR_MAP = {
    '=': operator.eq,
    '<': operator.lt,
    '>': operator.gt,
    '<=': operator.le,
    '>=': operator.ge,
    '<>': operator.ne,
    '~~': like_op,
    '~~*': ilike_op,
    '!~~*': not_ilike_op,
    '!~~': not_like_op,
}


class PartitionMsgpackForeignDataWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(PartitionMsgpackForeignDataWrapper, self).__init__(options, columns)
        self.columns = columns
        if 'filename' not in options:
            log_to_postgres(
                'Filename is required option of the partition msgpack fdw.',
                ERROR,
                hint='Try adding the missing option in the table creation statement')  # FIXME:
            raise RuntimeError('filename is required option of the partition msgpack fdw.')
        self.filename = options['filename']

    @staticmethod
    def decode_obj(obj):
        if b'__datetime__' in obj:
            # FIXME: not tested
            try:
                obj = datetime.strptime(obj['as_str'], DATETIME_FORMAT_NO_MS)
            except ValueError:
                # The preferred format is without the microseconds, but there are some lingering
                # bundle that still have it.
                obj = datetime.strptime(obj['as_str'], DATETIME_FORMAT_WITH_MS)
        elif b'__time__' in obj:
            # FIXME: not tested
            obj = time(*list(time.strptime(obj['as_str'], TIME_FORMAT))[3:6])
        elif b'__date__' in obj:
            # FIXME: not tested
            obj = datetime.strptime(obj['as_str'], DATE_FORMAT).date()
        else:
            # FIXME: not tested
            raise Exception('Unknown type on decode: {} '.format(obj))
        return obj

    def _matches(self, quals, row):
        """ Returns True if row matches to all quals. Otherwise returns False.

        Args:
            quals (list of Qual):
            row (list or tuple):

        Returns:
            bool: True if row matches to all quals, False otherwise.
        """
        for qual in quals:
            op = QUAL_OPERATOR_MAP.get(qual.operator)
            if op is None:
                log_to_postgres(
                    'Unknown operator {} in the {} qual. Row will be returned.'.format(qual.operator, qual),
                    WARNING,
                    hint='Implement that operator in the ambryfdw wrapper.')
                continue

            elem_index = self.columns.index(qual.field_name)
            if not op(row[elem_index], qual.value):
                return False
        return True

    def execute(self, quals, columns):
        with open(self.filename) as stream:
            unpacker = msgpack.Unpacker(GzipFile(fileobj=stream), object_hook=self.decode_obj)
            header = None

            for row in unpacker:
                assert isinstance(row, (tuple, list)), row

                if not header:
                    header = row
                    continue

                if not self._matches(quals, row):
                    continue

                yield row
