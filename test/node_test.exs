defmodule DistributedSpreadsheet.NodeTest do
  use ExUnit.Case

  alias DistributedSpreadsheet.Node



  test "application starts successfully" do
    cell = %{column: 1, line: 1}
    assert Node.get_cell_value(cell) == "Empty"
  end

  test "proposing a cell value updates the cell" do
    cell = %{column: 1, line: 1}
    value = "10"

    Node.propose_cell_value(cell, value)

    assert Node.get_cell_value(cell) == value
  end

  test "updating a cell value propagates to other nodes" do
    cell = %{column: 1, line: 2}
    value = "20"

    {:ok, _pid2} = Node.start_link(name: :node_test_instance_2)

    Node.propose_cell_value(cell, value)

    assert Node.get_cell_value(cell) == value
    assert Node.get_cell_value(cell) == value
  end

  test "cell updates are propagated correctly" do
    cell = %{column: 2, line: 2}
    value = "30"

    {:ok, _pid3} = Node.start_link(name: :node_test_instance_3)

    Node.propose_cell_value(cell, value)

    assert Node.get_cell_value(cell) == value
    assert Node.get_cell_value(cell) == value
  end
end
