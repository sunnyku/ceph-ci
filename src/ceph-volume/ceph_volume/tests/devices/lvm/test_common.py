from ceph_volume.devices.lvm import common


class TestRollbackOsd(object):

    def test_no_osd_id_does_nothing(self):
        result = common.rollback_osd(None, None)
        assert result is None

    def test_purging(self, capture_run, factory):
        args = factory(osd_id=None)
        common.rollback_osd(args, osd_id=0)
        expected = [
            'ceph', 'osd', 'purge', 'osd.0',
            '--yes-i-really-mean-it']

        assert capture_run.calls[0]['args'][0] == expected

    def test_destroying(self, capture_run, factory):
        args = factory(osd_id=0)
        common.rollback_osd(args, osd_id=0)
        expected = [
            'ceph', 'osd', 'destroy', 'osd.0',
            '--yes-i-really-mean-it']

        assert capture_run.calls[0]['args'][0] == expected

