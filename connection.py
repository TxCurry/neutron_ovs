import os
import threading
from ovs import poller
from _i18n import _
from six.moves import queue as Queue
from debtcollector import removals
from ovs.db import idl
import idlutils
from oslo_config import cfg
interface_map = {
    'vsctl': 'neutron.agent.ovsdb.impl_vsctl.OvsdbVsctl',
    'native': 'neutron.agent.ovsdb.impl_idl.NeutronOvsdbIdl',
}

OPTS = [
    cfg.StrOpt('ovsdb_interface',
               choices=interface_map.keys(),
               default='native',
               help=_('The interface for interacting with the OVSDB')),
    cfg.StrOpt('ovsdb_connection',
               default='tcp:172.16.30.40:6640',
               help=_('The connection string for the OVSDB backend. '
                      'Will be used by ovsdb-client when monitoring and '
                      'used for the all ovsdb commands when native '
                      'ovsdb_interface is enabled'
                      ))
]
cfg.CONF.import_opt('ovs_vsctl_timeout', 'ovs_lib')
cfg.CONF.register_opts(OPTS, 'OVS')


class TransactionQueue(Queue.Queue, object):
    def __init__(self, *args, **kwargs):
        super(TransactionQueue, self).__init__(*args, **kwargs)
        alertpipe = os.pipe()
        self.alertin = os.fdopen(alertpipe[0], 'rb', 0)
        self.alertout = os.fdopen(alertpipe[1], 'wb', 0)

    def get_nowait(self, *args, **kwargs):
        try:
            result = super(TransactionQueue, self).get_nowait(*args, **kwargs)
        except Queue.Empty:
            return None
        self.alertin.read(1)
        return result

    def put(self, *args, **kwargs):
        super(TransactionQueue, self).put(*args, **kwargs)
        self.alertout.write(six.b('X'))
        self.alertout.flush()

    @property
    def alert_fileno(self):
        return self.alertin.fileno()


class Connection(object):
    __rm_args = {'version': 'Ocata', 'removal_version': 'Pike',
                 'message': _('Use an idl_factory function instead')}

    @removals.removed_kwarg('connection', **__rm_args)
    @removals.removed_kwarg('schema_name', **__rm_args)
    @removals.removed_kwarg('idl_class', **__rm_args)
    def __init__(self, connection=None, timeout=None,schema_name=None,
                 idl_class=None, idl_factory=None):
        self.idl = None
        self.timeout = timeout
        self.txns = TransactionQueue(1)
        self.lock = threading.Lock()
        if idl_factory:
            if connection or schema_name:
                raise TypeError(_('Connection: Takes either idl_factory, or '
                                  'connection and schema_name. Both given'))
            self.idl_factory = idl_factory
        else:
            if not connection or not schema_name:
                raise TypeError(_('Connection: Takes either idl_factory, or '
                                  'connection and schema_name. Neither given'))
            self.idl_factory = self._idl_factory
            self.connection = connection
            self.schema_name = schema_name
            self.idl_class = idl_class or idl.Idl
            self._schema_filter = None

    @removals.remove(**__rm_args)
    def _idl_factory(self):
        helper = self.get_schema_helper()
        self.update_schema_helper(helper)
        return self.idl_class(self.connection, helper)

    @removals.removed_kwarg('table_name_list', **__rm_args)
    def start(self, table_name_list=None):
        self._schema_filter = table_name_list
        with self.lock:
            if self.idl is not None:
                return

            self.idl = self.idl_factory()
            idlutils.wait_for_change(self.idl, self.timeout)
            self.poller = poller.Poller()
            self.thread = threading.Thread(target=self.run)
            self.thread.setDaemon(True)
            self.thread.start()

    @removals.remove(
        version='Ocata', removal_version='Pike',
        message=_("Use idlutils.get_schema_helper(conn, schema, retry=True)"))
    def get_schema_helper(self):
        """Retrieve the schema helper object from OVSDB"""
        return idlutils.get_schema_helper(self.connection, self.schema_name,
                                         retry=True)

    @removals.remove(
        version='Ocata', removal_version='Pike',
        message=_("Use an idl_factory and ovs.db.SchemaHelper for filtering"))
    def update_schema_helper(self, helper):
        if self._schema_filter:
            for table_name in self._schema_filter:
                helper.register_table(table_name)
        else:
            helper.register_all()

    def run(self):
        while True:
            self.idl.wait(self.poller)
            self.poller.fd_wait(self.txns.alert_fileno, poller.POLLIN)
            self.poller.timer_wait(self.timeout * 1000)
            self.poller.block()
            self.idl.run()
            txn = self.txns.get_nowait()
            if txn is not None:
                try:
                    txn.results.put(txn.do_commit())
                except Exception as ex:
                    er = idlutils.ExceptionResult(ex=ex,
                                                  tb=traceback.format_exc())
                    txn.results.put(er)
                self.txns.task_done()

    def queue_txn(self, txn):
        self.txns.put(txn)
    
    def db_list(self, table_name):
        table = self.idl.tables[table_name]
	row = table.rows.values()[0]
        columns = ['_uuid'] + list(table.columns.keys())
        for c in columns:
            print "%-30s: %s" % (c, idlutils.get_column_value(row, c))


    def get_table_name(self):
        self.start()
        table_list = self.idl.tables.keys()
	for i in range(len(table_list)):
            print(table_list[i])
        return table_list 

#if __name__ == "__main__":
    #ovsdb_connection = Connection(cfg.CONF.OVS.ovsdb_connection,cfg.CONF.ovs_vsctl_timeout,'Open_vSwitch')
    #table_list = ovsdb_connection.get_table_name()
    #print '\n'
    #ovsdb_connection.db_list(table_list[0])
