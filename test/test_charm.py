#!/usr/bin/env python3

import unittest
import sys
sys.path.append('lib')  # noqa
sys.path.append('src')  # noqa

from ops.framework import StoredState
from ops import testing
from ops.model import ActiveStatus, BlockedStatus

from charm import CockroachDbCharm
from db_instance_manager import DbInstanceManager, DbInstanceManagerEvents


class TestDbInstanceManager(DbInstanceManager):
    """A type used to replace DbInstanceManager during unit testing.

    It overrides methods that affect system state while leaving the rest of the
    conditional logic untouched.
    """

    on = DbInstanceManagerEvents()
    _stored = StoredState()

    def install(self):
        self._install_called = True
        super().install()

    def reconfigure(self):
        self._reconfigure_called = True
        super().reconfigure()

    def _install_binary(self):
        pass

    def _run_start(self):
        pass

    def _run_init(self):
        pass

    def _setup_systemd_service(self):
        pass

    def _get_cluster_id(self):
        return '71edcae1-bf9c-4935-879e-bb380df72a32'


class TestCockroachDbCharm(unittest.TestCase):

    def setUp(self):
        self.harness = testing.Harness(CockroachDbCharm)
        # Inject a dummy instance manager so that it doesn't try to modify system state.
        self.harness._charm_cls.instance_manager_cls = TestDbInstanceManager
        self.harness.update_config({
            'version': 'v19.2.2',
            'default-zone-replicas': 0,
            'system-data-replicas': 0,
        })

    def test_install(self):
        self.harness.begin()
        self.harness.charm.on.install.emit()
        self.assertTrue(self.harness.charm.instance_manager._install_called)

    def test_reconfigure(self):
        self.harness.begin()
        self.harness.charm.on.config_changed.emit()
        self.assertTrue(self.harness.charm.instance_manager._reconfigure_called)

    def test_db_initialized_on_start_single(self):
        self.harness.update_config({
            'version': 'v19.2.2',
            'default-zone-replicas': 1,
            'system-data-replicas': 1,
        })
        self.harness.set_leader()

        # TODO: remove this line once https://github.com/canonical/operator/pull/196 is merged.
        self.harness.add_relation('cluster', 'cockroachdb')

        self.harness.begin()
        self.assertFalse(self.harness.charm.cluster.is_cluster_initialized)
        self.harness.charm.on.start.emit()
        self.assertTrue(self.harness.charm.cluster.is_cluster_initialized)

    def test_db_initialized_on_start_single_extra_unit(self):
        self.harness.update_config({
            'version': 'v19.2.2',
            'default-zone-replicas': 1,
            'system-data-replicas': 1,
        })
        relation_id = self.harness.add_relation('cluster', 'cockroachdb')
        self.harness.update_relation_data(
            relation_id, 'cockroachdb', {
                'initial_unit': 'cockroachdb/1',
                'cluster_id': '71edcae1-bf9c-4935-879e-bb380df72a32'
            })
        self.harness.update_relation_data(
            relation_id, 'cockroachdb/0', {'ingress-address': '192.0.2.1'})
        self.harness.add_relation_unit(relation_id, 'cockroachdb/1',
                                       {'ingress-address': '192.0.2.2'})

        self.harness.begin()
        self.assertTrue(self.harness.charm.cluster.is_cluster_initialized)
        self.harness.charm.on.start.emit()
        self.assertIsInstance(self.harness.charm.unit.status, BlockedStatus)

    def test_db_initialized_on_start_ha_leader_late_peers(self):
        self.harness.update_config({
            'version': 'v19.2.2',
            'default-zone-replicas': 3,
            'system-data-replicas': 3,
        })
        self.harness.set_leader()

        # TODO: remove this line once https://github.com/canonical/operator/pull/196 is merged.
        relation_id = self.harness.add_relation('cluster', 'cockroachdb')

        self.harness.begin()

        self.harness.charm.on.start.emit()
        # TODO: restore the following lines once PR #196 is merged.
        # self.assertFalse(self.harness.charm.cluster.is_cluster_initialized)
        # self.assertIsInstance(self.harness.charm.unit.status, WaitingStatus)

        # TODO: restore this line once https://github.com/canonical/operator/pull/196 is merged.
        # relation_id = self.harness.add_relation('cluster', 'cockroachdb')
        self.harness.update_relation_data(
            relation_id, 'cockroachdb/0', {'ingress-address': '192.0.2.1'})
        self.harness.add_relation_unit(relation_id, 'cockroachdb/1',
                                       {'ingress-address': '192.0.2.2'})
        self.harness.add_relation_unit(relation_id, 'cockroachdb/2',
                                       {'ingress-address': '192.0.2.3'})

        self.harness.charm.on.start.emit()
        self.assertTrue(self.harness.charm.cluster.is_cluster_initialized)
        self.assertEqual(self.harness.charm.cluster.initial_unit,
                         self.harness.charm.model.unit.name)
        self.assertIsInstance(self.harness.charm.unit.status, ActiveStatus)

    def test_db_initialized_on_start_ha_leader_early_peers(self):
        self.harness.update_config({
            'version': 'v19.2.2',
            'default-zone-replicas': 3,
            'system-data-replicas': 3,
        })
        self.harness.set_leader()
        relation_id = self.harness.add_relation('cluster', 'cockroachdb')
        self.harness.update_relation_data(
            relation_id, 'cockroachdb/0', {'ingress-address': '192.0.2.1'})
        self.harness.add_relation_unit(relation_id, 'cockroachdb/1',
                                       {'ingress-address': '192.0.2.2'})
        self.harness.add_relation_unit(relation_id, 'cockroachdb/2',
                                       {'ingress-address': '192.0.2.3'})
        self.harness.begin()

        self.harness.charm.on.start.emit()
        self.assertTrue(self.harness.charm.cluster.is_cluster_initialized)
        self.assertIsInstance(self.harness.charm.unit.status, ActiveStatus)

        self.harness.charm.on.start.emit()
        self.assertTrue(self.harness.charm.cluster.is_cluster_initialized)
        self.assertEqual(self.harness.charm.cluster.initial_unit,
                         self.harness.charm.model.unit.name)
        self.assertIsInstance(self.harness.charm.unit.status, ActiveStatus)

    def test_init_db_single(self):
        self.harness.update_config({
            'version': 'v19.2.2',
            'default-zone-replicas': 1,
            'system-data-replicas': 1,
        })
        self.harness.set_leader(True)
        self.harness.begin()
        with self.assertRaises(RuntimeError):
            self.harness.charm.instance_manager.init_db()


if __name__ == "__main__":
    unittest.main()
