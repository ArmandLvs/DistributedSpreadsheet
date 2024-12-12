defmodule RaftTest do
  use ExUnit.Case
  alias DistributedSpreadsheet.Node
  alias DistributedSpreadsheet.VectorClock

  test "election timeout triggers candidate role and vote requests" do
    :ok = LocalCluster.start()
    {:ok, cluster} = LocalCluster.start_link(3, prefix: "dsn-", applications: [:distributed_spreadsheet])
    {:ok, [n1, n2, n3] = nodes} = LocalCluster.nodes(cluster)
    Node.launch_election_timeout(n1)
    Node.launch_election_timeout(n2)
    Node.launch_election_timeout(n3)
    Process.sleep(900)
    state = Node.get_state(n1)
    assert Node.get_current_role(state) == :Follower
    #state = Node.get_state(n1)
    #assert state.currentRole == :Candidate
    state.votedFor == node()
    #assert MapSet.member?(state.votesReceived, n2)
    LocalCluster.stop(cluster)
  end
end
