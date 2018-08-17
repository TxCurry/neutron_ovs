from ovs import stream
from ovs import jsonrpc
import os
from ovs.db import idl
from oslo_utils import excutils
from neutron_lib import exceptions
import tenacity
import time
from ovs import poller

class ExceptionResult(object):
    def __init__(self, ex, tb):
        self.ex = ex
        self.tb = tb

def get_schema_helper(connection, schema_name, retry=True):
    try:
        return _get_schema_helper(connection, schema_name)
    except Exception:
        with excutils.save_and_reraise_exception(reraise=False) as ctx:
            if not retry:
                ctx.reraise = True
            # We may have failed due to set-manager not being called
            helpers.enable_connection_uri(connection, set_timeout=True)

            # There is a small window for a race, so retry up to a second
            @tenacity.retry(wait=tenacity.wait_exponential(multiplier=0.01),
                            stop=tenacity.stop_after_delay(1),
                            reraise=True)
            def do_get_schema_helper():
                return _get_schema_helper(connection, schema_name)

            return do_get_schema_helper()

def _get_schema_helper(connection, schema_name):
    err, strm = stream.Stream.open_block(
        stream.Stream.open(connection))
    if err:
        raise Exception(_("Could not connect to %s") % connection)
    rpc = jsonrpc.Connection(strm)
    req = jsonrpc.Message.create_request('get_schema', [schema_name])
    err, resp = rpc.transact_block(req)
    rpc.close()
    if err:
        raise Exception(_("Could not retrieve schema from %(conn)s: "
                          "%(err)s") % {'conn': connection,
                                        'err': os.strerror(err)})
    elif resp.error:
        raise Exception(resp.error)
    return idl.SchemaHelper(None, resp.result)

def wait_for_change(_idl, timeout, seqno=None):
    if seqno is None:
        seqno = _idl.change_seqno
    stop = time.time() + timeout
    while _idl.change_seqno == seqno and not _idl.run():
        ovs_poller = poller.Poller()
        _idl.wait(ovs_poller)
        ovs_poller.timer_wait(timeout * 1000)
        ovs_poller.block()
        if time.time() > stop:
            raise Exception(_("Timeout"))

def get_column_value(row, col):
    """Retrieve column value from the given row.

    If column's type is optional, the value will be returned as a single
    element instead of a list of length 1.
    """
    if col == '_uuid':
        val = row.uuid
    else:
        val = getattr(row, col)

    # Idl returns lists of Rows where ovs-vsctl returns lists of UUIDs
    if isinstance(val, list) and len(val):
        if isinstance(val[0], idl.Row):
            val = [str(v.uuid) for v in val]
        col_type = row._table.columns[col].type
        # ovs-vsctl treats lists of 1 as single results
        if col_type.is_optional():
            val = val[0]
    return val
