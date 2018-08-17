from oslo_config import cfg
import connection
import sys
def main():
    ovsdb_connection = connection.Connection(cfg.CONF.OVS.ovsdb_connection,cfg.CONF.ovs_vsctl_timeout,'Open_vSwitch')
    table_list = ovsdb_connection.get_table_name()
    print '\n'
    ovsdb_connection.db_list(table_list[0])

if __name__ == '__main__':
    sys.exit(main())

