defmodule DistributedSpreadsheet.NodeTest do
  use ExUnit.Case
  alias DistributedSpreadsheet.Node

  test "application starts successfully" do
    :ok = LocalCluster.start()
    {:ok, cluster} = LocalCluster.start_link(3, prefix: "dsn-", applications: [:distributed_spreadsheet])
    {:ok, [n1, n2, n3] = nodes} = LocalCluster.nodes(cluster)
    cell = %{column: 1, line: 1}
    assert Node.get_cell_value(n1,cell) == "Empty"
    LocalCluster.stop(cluster)
  end

  test "proposing a cell value updates the cell" do
    :ok = LocalCluster.start()
    {:ok, cluster} = LocalCluster.start_link(3, prefix: "dsn-", applications: [:distributed_spreadsheet])
    {:ok, [n1, n2, n3] = nodes} = LocalCluster.nodes(cluster)
    cell = %{column: 1, line: 1}
    value = "10"

    Node.propose_cell_value(n1,cell, value)

    assert Node.get_cell_value(n1,cell) == value
    LocalCluster.stop(cluster)
  end

  test "updating a cell value propagates to other nodes" do
    :ok = LocalCluster.start()
    {:ok, cluster} = LocalCluster.start_link(3, prefix: "dsn-", applications: [:distributed_spreadsheet])
    {:ok, [n1, n2, n3] = nodes} = LocalCluster.nodes(cluster)
    cell = %{column: 1, line: 2}
    value = "20"
    Node.propose_cell_value(n1,cell, value)

    Process.sleep(9000)

    assert Node.get_cell_value(n3,cell) == value
    LocalCluster.stop(cluster)
  end

  test "cell updates are propagated correctly" do
    :ok = LocalCluster.start()
    {:ok, cluster} = LocalCluster.start_link(3, prefix: "dsn-", applications: [:distributed_spreadsheet])
    {:ok, [n1, n2, n3] = nodes} = LocalCluster.nodes(cluster)
    cell = %{column: 2, line: 2}
    value = "30"
    LocalCluster.nodes(cluster)
    Node.propose_cell_value(n1,cell, value)
    Process.sleep(9000)

    assert Node.get_cell_value(n2,cell) == value
    assert Node.get_cell_value(n3,cell) == value

    cell = %{column: 10, line: 9}
    value = "20"
    LocalCluster.nodes(cluster)
    Node.propose_cell_value(n1,cell, value)
    Process.sleep(9000)

    assert Node.get_cell_value(n2,cell) == value
    assert Node.get_cell_value(n3,cell) == value

    cell = %{column: 10, line: 9}
    value = "10"
    LocalCluster.nodes(cluster)
    Node.propose_cell_value(n2,cell, value)
    Process.sleep(9000)

    assert Node.get_cell_value(n1,cell) == value
    assert Node.get_cell_value(n2,cell) == value
    assert Node.get_cell_value(n3,cell) == value
    LocalCluster.stop(cluster)
  end


end
