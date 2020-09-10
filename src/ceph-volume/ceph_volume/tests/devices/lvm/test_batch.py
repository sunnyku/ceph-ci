import pytest
import random
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

    @pytest.mark.parametrize('format_', ['pretty', 'json', 'json-pretty'])
    def test_json_report(self, format_, factory, conf_ceph_stub, mock_device_generator):
        # just ensure reporting works
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        devs = [mock_device_generator() for _ in range(5)]
        args = factory(data_slots=1,
                       osds_per_device=1,
                       osd_ids=[],
                       report=True,
                       format=format_,
                       devices=devs,
                       db_devices=[],
                       wal_devices=[],
                       bluestore=True,
                       block_db_size="1G",
                       dmcrypt=True,
                      )
        b = batch.Batch([])
        plan = b.get_plan(args)
        b.args = args
        b.report(plan)

    def test_get_physical_osds_return_len(self, factory,
                                          mock_devices_available,
                                          conf_ceph_stub,
                                          osds_per_device):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        args = factory(data_slots=1, osds_per_device=osds_per_device,
                       osd_ids=[], dmcrypt=False)
        osds = batch.get_physical_osds(mock_devices_available, args)
        assert len(osds) == len(mock_devices_available) * osds_per_device

    def test_get_physical_osds_rel_size(self, factory,
                                          mock_devices_available,
                                          conf_ceph_stub,
                                          osds_per_device):
        args = factory(data_slots=1, osds_per_device=osds_per_device,
                       osd_ids=[], dmcrypt=False)
        osds = batch.get_physical_osds(mock_devices_available, args)
        for osd in osds:
            assert osd.data[1] == 1.0 / osds_per_device

    def test_get_physical_osds_abs_size(self, factory,
                                          mock_devices_available,
                                          conf_ceph_stub,
                                          osds_per_device):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        args = factory(data_slots=1, osds_per_device=osds_per_device,
                       osd_ids=[], dmcrypt=False)
        osds = batch.get_physical_osds(mock_devices_available, args)
        for osd, dev in zip(osds, mock_devices_available):
            assert osd.data[2] == int(dev.vg_size[0] / osds_per_device)

    def test_get_physical_osds_osd_ids(self, factory,
                                          mock_devices_available,
                                          osds_per_device):
        pass

    def test_get_physical_fast_allocs_length(self, factory,
                                             conf_ceph_stub,
                                             mock_devices_available):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        args = factory(block_db_slots=None, get_block_db_size=None)
        fast = batch.get_physical_fast_allocs(mock_devices_available,
                                              'block_db', 2, 2, args)
        assert len(fast) == 2

    @pytest.mark.parametrize('occupied_prior', range(7))
    @pytest.mark.parametrize('slots,num_devs',
                             [l for sub in [list(zip([x]*x, range(1, x + 1))) for x in range(1,7)] for l in sub])
    def test_get_physical_fast_allocs_length_existing(self,
                                                      num_devs,
                                                      slots,
                                                      occupied_prior,
                                                      factory,
                                                      conf_ceph_stub,
                                                      mock_device_generator):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        occupied_prior = min(occupied_prior, slots)
        devs = [mock_device_generator() for _ in range(num_devs)]
        already_assigned = 0
        while already_assigned < occupied_prior:
            dev_i = random.randint(0, num_devs - 1)
            dev = devs[dev_i]
            if len(dev.lvs) < occupied_prior:
                dev.lvs.append('foo')
                dev.path = '/dev/bar'
                already_assigned = sum([len(d.lvs) for d in devs])
        args = factory(block_db_slots=None, get_block_db_size=None)
        expected_num_osds = max(len(devs) * slots - occupied_prior, 0)
        fast = batch.get_physical_fast_allocs(devs,
                                              'block_db', slots,
                                              expected_num_osds, args)
        assert len(fast) == expected_num_osds
        expected_assignment_on_used_devices = sum([slots - len(d.lvs) for d in devs if len(d.lvs) > 0])
        assert len([f for f in fast if f[0] == '/dev/bar']) == expected_assignment_on_used_devices
        assert len([f for f in fast if f[0] != '/dev/bar']) == expected_num_osds - expected_assignment_on_used_devices

    def test_get_lvm_osds_return_len(self, factory,
                                     mock_lv_device_generator,
                                     conf_ceph_stub,
                                     osds_per_device):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        args = factory(data_slots=1, osds_per_device=osds_per_device,
                       osd_ids=[], dmcrypt=False)
        mock_lvs = [mock_lv_device_generator()]
        osds = batch.get_lvm_osds(mock_lvs, args)
        assert len(osds) == 1
