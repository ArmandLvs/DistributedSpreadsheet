defmodule DistributedSpreadsheet.Node do
  use GenServer
  require Logger

  defmodule State do
    @enforce_keys [:cells]
    defstruct cells: %{}

    @type t() :: %__MODULE__{cells: %{optional(any()) => any()}}
  end

  ### Interface

  @spec start_link :: :ignore | {:error, any} | {:ok, pid}
  def start_link(), do: GenServer.start_link(__MODULE__, {}, name: __MODULE__)

  @spec start_link(keyword()) :: {:ok, pid} | {:error, any}
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, opts)
  end

  @spec propose_cell_value(cell :: any(), value :: any()) :: :ok
  def propose_cell_value(cell, value) do
    GenServer.cast(__MODULE__, {:propose_cell_value, cell, value})
  end

  @spec propose_cell_value(node(), cell :: any(), value :: any()) :: :ok
  def propose_cell_value(node, cell, value) do
    GenServer.cast({__MODULE__, node}, {:propose_cell_value, cell, value})
  end

  @spec get_cell_value(cell :: any()) :: any()
  def get_cell_value(cell) do
    GenServer.call(__MODULE__, {:get_cell_value, cell})
  end

  @spec get_cell_value(node(), cell :: any()) :: any()
  def get_cell_value(node, cell) do
    GenServer.call({__MODULE__, node}, {:get_cell_value, cell})
  end

  ### Callbacks

  @impl true
  def init(_) do
    :ok = :syn.join(:distributed_spreadsheet, :node, self())
    Logger.debug("Node started and joined :distributed_spreadsheet scope.")
    {:ok, %State{cells: %{}}}
  end

  @impl true
  def handle_call({:get_cell_value, cell}, _from, %State{cells: cells} = state) do
    value = Map.get(cells, cell, "Empty")
    {:reply, value, state}
  end

  @impl true
  def handle_cast({:propose_cell_value, cell, value}, %State{cells: cells} = state) do
    new_state = %State{state | cells: Map.put(cells, cell, value)}
    broadcast_cell_update(cell, value)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:update_cell, cell, value}, %State{cells: cells} = state) do
    new_state = %State{state | cells: Map.put(cells, cell, value)}
    {:noreply, new_state}
  end

  ### Private Functions

  defp broadcast_cell_update(cell, value) do
    members = for {pid, _} <- :syn.members(:distributed_spreadsheet, :node), pid != self(), do: pid
    Enum.each(members, fn pid -> GenServer.cast(pid, {:update_cell, cell, value}) end)
  end
end
