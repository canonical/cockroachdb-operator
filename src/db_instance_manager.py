import subprocess
import os
import re
import pwd

from datetime import timedelta
from time import sleep

from ops.model import ModelError
from ops.charm import CharmEvents
from ops.framework import (
    Object,
    StoredState,
    EventBase,
    EventSource,
)

from jinja2 import Environment, FileSystemLoader


class ClusterInitializedEvent(EventBase):

    def __init__(self, handle, cluster_id):
        super().__init__(handle)
        self.cluster_id = cluster_id

    def snapshot(self):
        return self.cluster_id

    def restore(self, cluster_id):
        self.cluster_id = cluster_id


class DaemonStartedEvent(EventBase):
    """Emitted when a database daemon is started by the charm."""


class DbInstanceManagerEvents(CharmEvents):
    daemon_started = EventSource(DaemonStartedEvent)
    cluster_initialized = EventSource(ClusterInitializedEvent)


class DbInstanceManager(Object):
    """Responsible for managing machine state related to a database instance."""

    on = DbInstanceManagerEvents()

    _stored = StoredState()

    COCKROACHDB_SERVICE = 'cockroachdb.service'
    SYSTEMD_SERVICE_FILE = f'/etc/systemd/system/{COCKROACHDB_SERVICE}'
    WORKING_DIRECTORY = '/var/lib/cockroach'
    COCKROACH_INSTALL_DIR = '/usr/local/bin'
    COCKROACH_BINARY_PATH = f'{COCKROACH_INSTALL_DIR}/cockroach'
    COCKROACH_USERNAME = 'cockroach'
    MAX_RETRIES = 10
    RETRY_TIMEOUT = timedelta(milliseconds=125)

    def __init__(self, charm, key, is_single_node, cluster):
        super().__init__(charm, key)
        self._stored.set_default(is_started=False)
        self._stored.set_default(is_initialized=False)
        self._is_single_node = is_single_node
        self._cluster = cluster

    def install(self):
        self._install_binary()
        self._setup_systemd_service()

    def _install_binary(self):
        """Install CockroachDB from a resource or download a binary."""
        try:
            resource_path = self.model.resources.fetch('cockroach-linux-amd64')
        except ModelError:
            resource_path = None

        if resource_path is None:
            architecture = 'amd64'  # hard-coded until it becomes important
            version = self.model.config['version']
            cmd = (f'wget -qO- https://binaries.cockroachdb.com/'
                   f'cockroach-{version}.linux-{architecture}.tgz'
                   f'| tar -C {self.COCKROACH_INSTALL_DIR} -xvz --wildcards'
                   ' --strip-components 1 --no-anchored "cockroach*/cockroach"')
            subprocess.check_call(cmd, shell=True)
            os.chown(self.COCKROACH_BINARY_PATH, 0, 0)
        else:
            cmd = ['tar', '-C', self.COCKROACH_INSTALL_DIR, '-xv', '--wildcards',
                   '--strip-components', '1', '--no-anchored', 'cockroach*/cockroach',
                   '-zf', str(resource_path)]
            subprocess.check_call(cmd)

    def start(self):
        """Start the CockroachDB daemon.

        Starting the daemon for the first time in the single instance mode also initializes the
        database on-disk state.
        """
        self._run_start()
        self._stored.is_started = True
        if self._is_single_node and not self._stored.is_initialized:
            self._stored.is_initialized = self._stored.is_initialized = True
            self.on.cluster_initialized.emit(self._get_cluster_id())
        self.on.daemon_started.emit()

    def _run_start(self):
        subprocess.check_call(['systemctl', 'start', f'{self.COCKROACHDB_SERVICE}'])

    def init_db(self):
        if self._is_single_node:
            raise RuntimeError('tried to initialize a database in a single unit mode')
        elif not self.model.unit.is_leader():
            raise RuntimeError('tried to initialize a database as a minion')
        self._run_init()
        self.on.cluster_initialized.emit(self._get_cluster_id())

    def _run_init(self):
        subprocess.check_call([self.COCKROACH_BINARY_PATH, 'init', '--insecure'])

    def reconfigure(self):
        # TODO: handle real changes here like changing the replication factors via cockroach sql
        # TODO: emit daemon_started when a database is restarted.
        self._setup_systemd_service()

    @property
    def is_started(self):
        return self._stored.is_started

    def _get_cluster_id(self):
        for _ in range(self.MAX_RETRIES):
            res = subprocess.run([self.COCKROACH_BINARY_PATH, 'debug', 'gossip-values',
                                  '--insecure'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if not res.returncode:
                out = res.stdout.decode('utf-8')
                break
            elif not re.findall(r'code = Unavailable desc = node waiting for init',
                                res.stderr.decode('utf-8')):
                raise RuntimeError(
                    'unexpected error returned while trying to obtain gossip-values')
            sleep(self.RETRY_TIMEOUT.total_seconds())

        cluster_id_regex = re.compile(r'"cluster-id": (?P<uuid>[0-9a-fA-F]{8}\-[0-9a-fA-F]'
                                      r'{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12})$')
        for line in out.split('\n'):
            m = cluster_id_regex.match(line)
            if m:
                return m.group('uuid')
        raise RuntimeError('could not find cluster-id in the gossip-values output')

    def _setup_systemd_service(self):
        if self._is_single_node:
            # start-single-node will set replication factors for all zones to 1.
            exec_start_line = (f'ExecStart={self.COCKROACH_BINARY_PATH} start-single-node'
                               ' --advertise-addr {self._cluster.advertise_addr} --insecure')
        else:
            peer_addresses = [self._cluster.advertise_addr]
            if self._cluster.is_joined:
                peer_addresses.extend(self._cluster.peer_addresses)
            join_addresses = ','.join([str(a) for a in peer_addresses])
            # --insecure until the charm gets CA setup support figured out.
            exec_start_line = (f'ExecStart={self.COCKROACH_BINARY_PATH} start --insecure '
                               f'--advertise-addr={self._cluster.advertise_addr} '
                               f'--join={join_addresses}')
        ctxt = {
            'working_directory': self.WORKING_DIRECTORY,
            'exec_start_line': exec_start_line,
        }
        env = Environment(loader=FileSystemLoader('templates'))
        template = env.get_template('cockroachdb.service')
        rendered_content = template.render(ctxt)

        content_hash = hash(rendered_content)
        # TODO: read the rendered file instead to account for any manual edits.
        old_hash = getattr(self._stored, 'rendered_content_hash', None)

        if old_hash is None or old_hash != content_hash:
            self._stored.rendered_content_hash = content_hash
            with open(self.SYSTEMD_SERVICE_FILE, 'wb') as f:
                f.write(rendered_content.encode('utf-8'))
            subprocess.check_call(['systemctl', 'daemon-reload'])

            try:
                pwd.getpwnam(self.COCKROACH_USERNAME)
            except KeyError:
                subprocess.check_call(['useradd',
                                       '-m',
                                       '--home-dir',
                                       self.WORKING_DIRECTORY,
                                       '--shell',
                                       '/usr/sbin/nologin',
                                       self.COCKROACH_USERNAME])

            if self._stored.is_started:
                subprocess.check_call(['systemctl', 'restart', f'{self.COCKROACHDB_SERVICE}'])
