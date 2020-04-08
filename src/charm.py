#!/usr/bin/env python3

import sys
sys.path.append('lib')  # noqa

from ops.charm import CharmBase
from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    WaitingStatus,
)

from db_instance_manager import DbInstanceManager
from cluster import CockroachDbCluster


class CockroachDbCharm(CharmBase):

    PSQL_PORT = 26257
    HTTP_PORT = 8080

    # A type to use for the database instance manager. The class attribute is
    # used to inject a different type during unit testing.
    instance_manager_cls = DbInstanceManager

    def __init__(self, framework, key):
        super().__init__(framework, key)

        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.cluster_relation_changed, self._on_cluster_relation_changed)

        self.cluster = CockroachDbCluster(self, 'cluster')
        self.instance_manager = self.instance_manager_cls(
            self, None, self.is_single_node, self.cluster)
        self.framework.observe(self.instance_manager.on.cluster_initialized,
                               self.cluster.on_cluster_initialized)

        self.framework.observe(self.instance_manager.on.daemon_started, self._on_daemon_started)

    def _on_install(self, event):
        self.instance_manager.install()

    @property
    def is_single_node(self):
        """Both replication factors were set to 1 so it's a good guess that an operator wants
        a 1-node deployment."""
        default_zone_rf = self.model.config['default-zone-replicas']
        system_data_rf = self.model.config['system-data-replicas']
        return default_zone_rf == 1 and system_data_rf == 1

    def _on_start(self, event):
        # If both replication factors are set to 1 and the current unit != initial cluster unit,
        # don't start the process if the cluster has already been initialized.
        # This configuration is not practical in real deployments (i.e. multiple units, RF=1).
        initial_unit = self.cluster.initial_unit
        if self.is_single_node and (
                initial_unit is not None and self.unit.name != initial_unit):
            self.unit.status = BlockedStatus('Extra unit in a single-node deployment.')
            return
        self.instance_manager.start()

        if self.cluster.is_joined and self.cluster.is_cluster_initialized:
            self.unit.status = ActiveStatus()

    def _on_cluster_relation_changed(self, event):
        self.instance_manager.reconfigure()
        if self.instance_manager.is_started and self.cluster.is_cluster_initialized:
            self.unit.status = ActiveStatus()

    def _on_daemon_started(self, event):
        if not self.cluster.is_joined and not self.is_single_node:
            self.unit.status = WaitingStatus('Waiting for peer units to join.')
            event.defer()
            return
        if self.cluster.is_cluster_initialized:
            # Skip this event when some other unit has already initialized a cluster.
            self.unit.status = ActiveStatus()
            return
        elif not self.unit.is_leader():
            self.unit.status = WaitingStatus(
                'Waiting for the leader unit to initialize a cluster.')
            event.defer()
            return
        self.unit.status = MaintenanceStatus('Initializing the cluster.')
        # Initialize the cluster if we're a leader in a multi-node deployment, otherwise it have
        # already been initialized by running start-single-node.
        if not self.is_single_node and self.model.unit.is_leader():
            self.instance_manager.init_db()

        self.unit.status = ActiveStatus()

    def _on_config_changed(self, event):
        self.instance_manager.reconfigure()


if __name__ == '__main__':
    main(CockroachDbCharm)
