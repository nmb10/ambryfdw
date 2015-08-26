
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
            'filename': os.path.join(TEST_FILES_DIR, 'header_2_columns_100_rows_all_integers.msg')}
        quals = []
        columns = ['a', 'b']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 100)
        self.assertEquals(ret[0], [0, 0])

    def test_generates_strings(self):
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'header_2_columns_100_rows_all_strings.msg')}
        quals = []
        columns = ['a', 'b']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 100)
        self.assertEquals(ret[0], ['0', '0'])

    @fudge.patch('ambryfdw.log_to_postgres')
    def test_logs_unknown_qual(self, fake_log):
        fake_log\
            .expects_call()\
            .with_args(arg.contains('Unknown operator'), WARNING, hint=arg.contains('Implement'))

        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'header_2_columns_100_rows_all_integers.msg')}
        columns = ['a', 'b']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        quals = [Qual('a', '?', 3)]
        list(wrapper.execute(quals, columns))

    def test_returns_less_then_records(self):
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'header_2_columns_100_rows_all_integers.msg')}
        columns = ['a', 'b']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        quals = [Qual('a', '<', 3)]
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 3)
        self.assertEquals(ret[0], [0, 0])
        self.assertEquals(ret[1], [1, 1])
        self.assertEquals(ret[2], [2, 2])

    def test_returns_greater_then_records(self):
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'header_2_columns_100_rows_all_integers.msg')}
        columns = ['a', 'b']
        wrapper = PartitionMsgpackForeignDataWrapper(options, columns)
        quals = [Qual('a', '>', 10)]
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 89)
        self.assertEquals(ret[0], [11, 11])
        self.assertEquals(ret[1], [12, 12])
        self.assertEquals(ret[2], [13, 13])

    # FIXME: Test date and time objects.
