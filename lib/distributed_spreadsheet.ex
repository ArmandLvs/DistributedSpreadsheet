defmodule DistributedSpreadsheet do
  @moduledoc """
  Documentation for `DistributedSpreadsheet`.
  """

  use Application

  def start(_start_mode, _start_arg) do
   # :ok = :node.add_scope(:distributed_spreadsheet)
    DistributedSpreadsheet.Sup.start_link()
  end
end
