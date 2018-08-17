from oslo_config import cfg

from _i18n import _

# Default timeout for ovs-vsctl command
DEFAULT_OVS_VSCTL_TIMEOUT = 10

OPTS = [
    cfg.IntOpt('ovs_vsctl_timeout',
               default=DEFAULT_OVS_VSCTL_TIMEOUT,
               help=_('Timeout in seconds for ovs-vsctl commands. '
                      'If the timeout expires, ovs commands will fail with '
                      'ALARMCLOCK error.')),
]


def register_ovs_agent_opts(cfg=cfg.CONF):
    cfg.register_opts(OPTS)
