defmodule DistributedSpreadsheet.Node do
  use GenServer
  require Logger

  defmodule State do
    @enforce_keys [:cells, :vector_clock]
    defstruct cells: %{}, vector_clock: DistributedSpreadsheet.VectorClock.new()

    @type t() :: %__MODULE__{
            cells: %{optional(any()) => any()},
            vector_clock: DistributedSpreadsheet.VectorClock.t()
          }
  end

  ### Interface

  @spec start_link :: :ignore | {:error, any} | {:ok, pid}
  def start_link(), do: GenServer.start_link(__MODULE__, {}, name: __MODULE__)

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

  @spec get_vector_clock() :: DistributedSpreadsheet.VectorClock.t()
  def get_vector_clock() do
    GenServer.call(__MODULE__, :get_vector_clock)
  end

  @spec get_vector_clock(node()) :: DistributedSpreadsheet.VectorClock.t()
  def get_vector_clock(node) do
    GenServer.call({__MODULE__, node}, :get_vector_clock)
  end

  ### Callbacks

  @impl true
  def init(_) do
    Logger.info("#{node()} started and joined cluster.")
    {:ok, %State{cells: %{}, vector_clock: DistributedSpreadsheet.VectorClock.new()}}
  end

  @impl true
  def handle_call({:get_cell_value, cell}, _from, %State{cells: cells} = state) do
    value = Map.get(cells, cell, "Empty")
    {:reply, value, state}
  end

  @impl true
  def handle_call(:get_vector_clock, _from, %State{vector_clock: vector_clock} = state) do
    {:reply, vector_clock, state}
  end

  @impl true
  def handle_cast({:propose_cell_value, cell, value}, %State{cells: cells, vector_clock: vc} = state) do
    updated_vc = DistributedSpreadsheet.VectorClock.increment_entry(vc, node())
    new_state = %State{state | cells: Map.put(cells, cell, value), vector_clock: updated_vc}
    broadcast_cell_update(cell, value, updated_vc)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:update_cell, cell, value, vector_clock}, %State{cells: cells, vector_clock: vc} = state) do
    new_vc = DistributedSpreadsheet.VectorClock.vmax(vector_clock, vc)
    updated_vc = DistributedSpreadsheet.VectorClock.increment_entry(new_vc, node())
    new_state = %State{state | cells: Map.put(cells, cell, value), vector_clock: updated_vc}
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:node_joined, node, vector_clock}, %State{vector_clock: vc} = state) do
    current_nodes = Map.keys(DistributedSpreadsheet.VectorClock.get_vector(vc))
    updated_vc = Enum.reduce(current_nodes, vector_clock, fn existing_node, clock ->
      DistributedSpreadsheet.VectorClock.increment_entry(clock, existing_node)
    end)
    final_vc = DistributedSpreadsheet.VectorClock.increment_entry(updated_vc, node)
    new_state = %State{state | vector_clock: final_vc}
    {:noreply, new_state}
  end

  ### Private Functions


  defp broadcast_cell_update(cell, value, vector_clock) do
    members = Node.list()
    Enum.each(members, fn node ->
      GenServer.cast({__MODULE__, node}, {:update_cell, cell, value, vector_clock})
    end)
  end

  defp broadcast_node_joined(node, vector_clock) do
    members = Node.list()
    Enum.each(members, fn member ->
      GenServer.cast({__MODULE__, member}, {:node_joined, node, vector_clock})
    end)
  end
end
