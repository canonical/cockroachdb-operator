#!/usr/bin/env python3

import unittest
import sys
sys.path.append('lib')  # noqa
sys.path.append('src')  # noqa

from ops import testing
from ops.charm import CharmBase, CharmEvents
from ops.framework import EventSource

from cluster import CockroachDbCluster
from db_instance_manager import ClusterInitializedEvent


class TestCharmEvents(CharmEvents):
    cluster_initialized = EventSource(ClusterInitializedEvent)


class TestCharm(CharmBase):
    on = TestCharmEvents()


class TestCockroachDBCluster(unittest.TestCase):

    def setUp(self):
        self.harness = testing.Harness(TestCharm, meta='''
            name: cockroachdb
            peers:
              cluster:
                interface: cockroachdb-peer
        ''')

        self.harness.begin()
        self.cluster = CockroachDbCluster(self.harness.charm, 'cluster')
        # A charm author is exptected to do that in the constructor so we mimic
        # this here.
        self.harness.framework.observe(self.harness.charm.on.cluster_initialized,
                                       self.cluster.on_cluster_initialized)

    def test_is_cluster_joined(self):
        relation_id = self.harness.add_relation('cluster', 'cockroachdb')
        self.harness.update_relation_data(
            relation_id, 'cockroachdb/0', {'ingress-address': '192.0.2.1'})
        self.assertTrue(self.cluster.is_joined)

    def test_is_single(self):
        relation_id = self.harness.add_relation('cluster', 'cockroachdb')
        self.harness.update_relation_data(
            relation_id, 'cockroachdb/0', {'ingress-address': '192.0.2.1'})
        self.assertTrue(self.cluster.is_single)

    def test_peer_addresses(self):
        relation_id = self.harness.add_relation('cluster', 'cockroachdb')
        self.harness.update_relation_data(
            relation_id, 'cockroachdb/0', {'ingress-address': '192.0.2.1'})
        self.harness.add_relation_unit(relation_id, 'cockroachdb/1',
                                       {'ingress-address': '192.0.2.2'})
        self.harness.add_relation_unit(relation_id, 'cockroachdb/2',
                                       {'ingress-address': '192.0.2.3'})
        # Relation units are stored in a set hence the result may not
        # always be ordered in the same way.
        self.assertEqual(set(self.cluster.peer_addresses), set(['192.0.2.2', '192.0.2.3']))

    def test_initial_unit(self):
        relation_id = self.harness.add_relation('cluster', 'cockroachdb')
        self.assertIsNone(self.cluster.initial_unit)

        self.harness.update_relation_data(
            relation_id, 'cockroachdb', {
                'cluster_id': '449ce7de-faea-48f1-925b-198032fdacc4',
                'initial_unit': 'cockroachdb/1'
            })
        self.assertEqual(self.cluster.initial_unit, 'cockroachdb/1')

    def test_advertise_addr(self):
        # TODO: implement when network_get gets implemented for the test harness.
        pass

    def test_on_cluster_initialized_when_joined(self):
        '''Test that the initial unit exposes a cluster id and reports the init state correctly.
        '''
        self.harness.set_leader()
        relation_id = self.harness.add_relation('cluster', 'cockroachdb')
        self.harness.update_relation_data(
            relation_id, 'cockroachdb/0', {'ingress-address': '192.0.2.1'})
        self.assertFalse(self.cluster.is_cluster_initialized)

        cluster_id = '449ce7de-faea-48f1-925b-198032fdacc4'
        self.harness.charm.on.cluster_initialized.emit(cluster_id)
        self.assertTrue(self.cluster.is_cluster_initialized)

        cluster_relation = self.harness.charm.model.get_relation('cluster')
        app_data = cluster_relation.data[self.harness.charm.app]
        self.assertEqual(app_data.get('cluster_id'), cluster_id)
        self.assertEqual(app_data.get('initial_unit'), self.harness.charm.unit.name)

# TODO: uncomment this once https://github.com/canonical/operator/pull/196 is merged.
#    def test_on_cluster_initialized_when_not_joined(self):
#        '''Test a scenario when an initial unit generates cluster state without a peer relation.
#
#        This situation occurs on versions of Juju that do not have relation-created hooks fired
#        before the start event.
#        '''
#        self.harness.set_leader()
#        self.assertFalse(self.cluster.is_cluster_initialized)
#
#        cluster_id = '449ce7de-faea-48f1-925b-198032fdacc4'
#        self.harness.charm.on.cluster_initialized.emit(cluster_id)
#        self.assertTrue(self.cluster.is_cluster_initialized)
#        self.assertTrue(self.cluster.stored.cluster_id, cluster_id)

    def test_on_cluster_initialized_not_leader(self):
        '''Test that the handler raises an exception if erroneously used from a non-leader unit.
        '''
        self.harness.set_leader(is_leader=False)
        relation_id = self.harness.add_relation('cluster', 'cockroachdb')
        self.harness.update_relation_data(
            relation_id, 'cockroachdb/0', {'ingress-address': '192.0.2.1'})
        self.assertFalse(self.cluster.is_cluster_initialized)

        with self.assertRaises(RuntimeError):
            self.harness.charm.on.cluster_initialized.emit('449ce7de-faea-48f1-925b-198032fdacc4')


if __name__ == "__main__":
    unittest.main()
