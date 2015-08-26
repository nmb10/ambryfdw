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
  The full path to the CSV file containing the data. This file must be readable
  to the postgres user.

Usage example
-------------

Supposing you want to parse the following msgpack partition file, located in ``/tmp/test.msg``::

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

from datetime import datetime, time
import operator

import msgpack

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING

QUAL_OPERATOR_MAP = {
    '>': operator.gt,
    '<': operator.lt,
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
                obj = datetime.strptime(obj['as_str'], '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                # The preferred format is without the microseconds, but there are some lingering
                # bundle that still have it.
                obj = datetime.datetime.strptime(obj['as_str'], '%Y-%m-%dT%H:%M:%S.%f')
        elif b'__time__' in obj:
            # FIXME: not tested
            obj = time(*list(time.strptime(obj['as_str'], '%H:%M:%S'))[3:6])
        elif b'__date__' in obj:
            # FIXME: not tested
            obj = datetime.datetime.strptime(obj['as_str'], '%Y-%m-%d').date()
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
        # FIXME: Implement quals
        with open(self.filename) as stream:
            unpacker = msgpack.Unpacker(stream, object_hook=self.decode_obj)
            header = None

            for row in unpacker:
                assert isinstance(row, (tuple, list)), row

                if not header:
                    header = row
                    continue

                if not self._matches(quals, row):
                    continue

                yield row
