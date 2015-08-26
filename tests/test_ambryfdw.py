
import os

from unittest import TestCase

from ambryfdw import PartitionMsgpackForeignDataWrapper


TEST_FILES_DIR = os.path.join(os.path.dirname(__file__), 'files')


class TestAmbryFdw(TestCase):
    def test_requires_filename(self):
        with self.assertRaises(RuntimeError):
            PartitionMsgpackForeignDataWrapper({}, [])

    def test_generates_integers(self):
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'header_2_columns_100_rows_all_integers.msg')}
        wrapper = PartitionMsgpackForeignDataWrapper(options, [])
        quals = None
        columns = ['a', 'b']
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 100)
        self.assertEquals(ret[0], [0, 0])

    def test_generates_strings(self):
        options = {
            'filename': os.path.join(TEST_FILES_DIR, 'header_2_columns_100_rows_all_strings.msg')}
        wrapper = PartitionMsgpackForeignDataWrapper(options, [])
        quals = None
        columns = ['a', 'b']
        ret = list(wrapper.execute(quals, columns))
        self.assertEquals(len(ret), 100)
        self.assertEquals(ret[0], ['0', '0'])

    # FIXME: Test date and time objects.
