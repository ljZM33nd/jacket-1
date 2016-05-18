from nova.compute import power_state
from nova.virt.aws import adapter

AWS_POWER_STATE={
    "RUNNING":power_state.RUNNING,
    "TERMINATED":power_state.CRASHED,
    "PENDING":power_state.NOSTATE,
    "UNKNOWN":power_state.NOSTATE,
    "STOPPED":power_state.SHUTDOWN,
}

class HCAWSS(object):
    def __init__(self, access_key_id, secret, region, secure):
        self.compute_adapter = adapter.Ec2Adapter(access_key_id, secret, region, secure)

    def synchronize_status(self):
        nodes_status = {}
        nodes = self.compute_adapter.list_nodes()
        for node in nodes:
            nodes_status[node.id] = unify_power_state(node.state)
        return nodes_status

    def unify_power_state(aws_status):
        if aws_status in AWS_POWER_STATE:
            return AWS_POWER_STATE.get(aws_status)

        return power_state.NOSTATE