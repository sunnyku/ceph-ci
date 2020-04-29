import pytest
import json
from ceph_volume.devices.lvm import batch


class TestBatch(object):

    def test_batch_instance(self, is_root):
        b = batch.Batch([])
        b.main()

    def test_get_devices(self, monkeypatch):
        return_value = {
            '/dev/vdd': {
                'removable': '0',
                'vendor': '0x1af4',
                'model': '',
                'sas_address': '',
                'sas_device_handle': '',
                'sectors': 0,
                'size': 21474836480.0,
                'support_discard': '',
                'partitions': {
                    'vdd1': {
                        'start': '2048',
                        'sectors': '41940959',
                        'sectorsize': 512,
                        'size': '20.00 GB'
                    }
                },
                'rotational': '1',
                'scheduler_mode': 'mq-deadline',
                'sectorsize': '512',
                'human_readable_size': '20.00 GB',
                'path': '/dev/vdd'
            },
            '/dev/vdf': {
                'removable': '0',
                'vendor': '0x1af4',
                'model': '',
                'sas_address': '',
                'sas_device_handle': '',
                'sectors': 0,
                'size': 21474836480.0,
                'support_discard': '',
                'partitions': {},
                'rotational': '1',
                'scheduler_mode': 'mq-deadline',
                'sectorsize': '512',
                'human_readable_size': '20.00 GB',
                'path': '/dev/vdf'
            }
        }
        monkeypatch.setattr('ceph_volume.devices.lvm.batch.disk.get_devices',
                            lambda: return_value)
        b = batch.Batch([])
        result = b.get_devices().strip()
        assert result == '* /dev/vdf                  20.00 GB   rotational'

    def test_disjoint_device_lists(self, factory):
        device1 = factory(used_by_ceph=False, available=True, abspath="/dev/sda")
        device2 = factory(used_by_ceph=False, available=True, abspath="/dev/sdb")
        devices = [device1, device2]
        db_devices = [device2]
        with pytest.raises(Exception) as disjoint_ex:
            batch.ensure_disjoint_device_lists(devices, db_devices)
        assert 'Device lists are not disjoint' in str(disjoint_ex.value)

    def test_get_physical_osds_return_len(self, factory,
                                          mock_devices_available):
        osds_per_device = 1
        args = factory(data_slots=1, osds_per_device=osds_per_device, osd_ids=[])
        osds = batch.get_physical_osds(mock_devices_available, args)
        assert len(osds) == len(mock_devices_available) * osds_per_device

    def test_get_physical_osds_rel_size(self, factory,
                                          mock_devices_available):
        osds_per_device = 1
        args = factory(data_slots=1, osds_per_device=osds_per_device, osd_ids=[])
        osds = batch.get_physical_osds(mock_devices_available, args)
        for osd in osds:
            assert osd.data[1] == 100 / osds_per_device

    def test_get_physical_osds_abs_size(self, factory,
                                          mock_devices_available):
        osds_per_device = 1
        args = factory(data_slots=1, osds_per_device=osds_per_device, osd_ids=[])
        osds = batch.get_physical_osds(mock_devices_available, args)
        for osd, dev in zip(osds, mock_devices_available):
            assert osd.data[2] == dev.vg_size[0] / osds_per_device
