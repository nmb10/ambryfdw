
from datetime import datetime, date
import os
from unittest import TestCase

import fudge
from fudge.inspector import arg

from multicorn import Qual
from multicorn.utils import WARNING

from ambryfdw import PartitionMsgpackForeignDataWrapper


TEST_FILES_DIR = os.path.join(os.path.dirname(__file__), 'files')


class TestAmbryFdw(TestCase):
    def test_requires_filename(self):
        with self.assertRaises(RuntimeError):
            PartitionMsgpackForeignDataWrapper({}, [])

    def test_generates_integers(self):
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'rowid_int_col1_int_header_and_100_rows_gzipped.msg')}
        quals = []
        columns = ['rowid', 'col1']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 100)
        self.assertEquals(ret[0], [0, 0])

    def test_generates_strings(self):
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'rowid_int_col1_str_header_and_100_rows_gzipped.msg')}
        quals = []
        columns = ['rowid', 'col1']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 100)
        self.assertEquals(ret[0], [0, '0'])

    def test_generates_dates(self):
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'rowid_int_col1_date_header_and_100_rows_gzipped.msg')}
        quals = []
        columns = ['rowid', 'col1']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 100)
        self.assertEquals(ret[0], [0, date(2015, 8, 30)])

    def test_generates_datetimes(self):
        options = {
            'filename': os.path.join(
                TEST_FILES_DIR,
                'rowid_int_col1_datetime_header_and_100_rows_gzipped.msg')}
        quals = []
        columns = ['rowid', 'col1']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 100)
        self.assertEquals(ret[0], [0, datetime(2015, 8, 30, 12, 9, 10, 681995)])

    @fudge.patch('ambryfdw.log_to_postgres')
    def test_logs_unknown_qual(self, fake_log):
        fake_log\
            .expects_call()\
            .with_args(arg.contains('Unknown operator'), WARNING, hint=arg.contains('Implement'))

        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'rowid_int_col1_str_header_and_100_rows_gzipped.msg')}
        columns = ['rowid', 'col1']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        quals = [Qual('col1', '?', '3')]
        list(wrapper.execute(quals, columns))

    # quals tests
    def test_equals_operator(self):
        """ tests = operator. """
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'rowid_int_col1_str_header_and_100_rows_gzipped.msg')}
        columns = ['rowid', 'col1']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        quals = [Qual('col1', '=', '3')]
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 1)
        self.assertEquals(ret[0], [3, '3'])

    def test_less_than_operator(self):
        """ tests < operator. """
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'rowid_int_col1_int_header_and_100_rows_gzipped.msg')}
        columns = ['rowid', 'col1']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        quals = [Qual('col1', '<', 3)]
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 3)
        self.assertEquals(ret[0], [0, 0])
        self.assertEquals(ret[1], [1, 1])
        self.assertEquals(ret[2], [2, 2])

    def test_greater_than_operator(self):
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'rowid_int_col1_int_header_and_100_rows_gzipped.msg')}
        columns = ['rowid', 'col1']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        quals = [Qual('col1', '>', 10)]
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 89)
        self.assertEquals(ret[0], [11, 11])
        self.assertEquals(ret[1], [12, 12])
        self.assertEquals(ret[2], [13, 13])

    def test_less_than_or_equal_operator(self):
        """ tests <= operator. """
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'rowid_int_col1_int_header_and_100_rows_gzipped.msg')}
        columns = ['rowid', 'col1']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        quals = [Qual('col1', '<=', 1)]
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 2)
        self.assertEquals(ret[0], [0, 0])
        self.assertEquals(ret[1], [1, 1])

    def test_greater_than_or_equal_operator(self):
        """ tests >= operator. """
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'rowid_int_col1_int_header_and_100_rows_gzipped.msg')}
        columns = ['rowid', 'col1']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        quals = [Qual('col1', '>=', 98)]
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 2)
        self.assertEquals(ret[0], [98, 98])
        self.assertEquals(ret[1], [99, 99])

    def test_not_equal_operator(self):
        """ tests <> operator. """
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'rowid_int_col1_int_header_and_100_rows_gzipped.msg')}
        columns = ['rowid', 'col1']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        quals = [Qual('col1', '<>', 0)]
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 99)
        self.assertEquals(ret[0], [1, 1])

    def test_like_operator(self):
        """ tests ~~ operator. """
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'rowid_int_col1_str_header_and_100_rows_gzipped.msg')}
        columns = ['rowid', 'col1']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)

        quals = [Qual('col1', '~~', '1')]
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 1)
        self.assertEquals(ret[0], [1, '1'])

        quals = [Qual('col1', '~~', '%1')]
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 10)
        self.assertEquals(ret[0], [1, '1'])
        self.assertEquals(ret[1], [11, '11'])
        self.assertEquals(ret[-1], [91, '91'])

        quals = [Qual('col1', '~~', '1%')]
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 11)
        self.assertEquals(ret[0], [1, '1'])
        self.assertEquals(ret[1], [10, '10'])
        self.assertEquals(ret[-1], [19, '19'])

        quals = [Qual('col1', '~~', '_')]
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 10)
        self.assertEquals(ret[0], [0, '0'])
        self.assertEquals(ret[1], [1, '1'])
        self.assertEquals(ret[-1], [9, '9'])
