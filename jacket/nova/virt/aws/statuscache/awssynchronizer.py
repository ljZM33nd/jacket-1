from libcloud.compute.types import NodeState
from nova.compute import power_state

AWS_POWER_STATE={
    NodeState.RUNNING:power_state.RUNNING,
    NodeState.TERMINATED:power_state.CRASHED,
    NodeState.PENDING:power_state.NOSTATE,
    NodeState.UNKNOWN:power_state.NOSTATE,
    NodeState.STOPPED:power_state.SHUTDOWN,
}

class HCAWSS(object):
    def __init__(self, adapter):
        self.compute_adapter = adapter

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
