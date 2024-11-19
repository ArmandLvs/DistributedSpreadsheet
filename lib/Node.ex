defmodule DistributedSpreadsheet.Node do
  use GenServer
  require Logger

  defmodule State do
    @enforce_keys [:cells, :vector_clock]
    defstruct cells: %{}, vector_clock: VectorClock.fresh()

    @type t() :: %__MODULE__{cells: %{optional(any()) => any()}, vector_clock: VectorClock.t()}
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

  ### Callbacks

  @impl true
  def init(_) do
    Logger.info("#{node()} started and joined cluster.")
    {:ok, %State{cells: %{}, vector_clock: VectorClock.fresh()}}
  end

  @impl true
  def handle_call({:get_cell_value, cell}, _from, %State{cells: cells} = state) do
    value = Map.get(cells, cell, "Empty")
    {:reply, value, state}
  end

  @impl true
  def handle_cast({:propose_cell_value, cell, value}, %State{cells: cells, vector_clock: vc} = state) do
    updated_vc = VectorClock.increment(vc, node())
    new_state = %State{state | cells: Map.put(cells, cell, value), vector_clock: updated_vc}
    broadcast_cell_update(cell, value, updated_vc)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:update_cell, cell, value, vector_clock}, %State{cells: cells, vector_clock: vc} = state) do
    new_vc = bigger_vector_clock(vector_clock,vc)
    new_state = %State{state | cells: Map.put(cells, cell, value), vector_clock: VectorClock.increment(new_vc, node())}
    {:noreply, new_state}
  end

  ### Private Functions

  defp bigger_vector_clock(vc1, vc2) do
    cond do
      VectorClock.dominates(vc1, vc2) -> vc1
      VectorClock.dominates(vc2, vc1) -> vc2
    end
  end

  defp broadcast_cell_update(cell, value, vector_clock) do
    members = Node.list()
    Enum.each(members, fn node ->
      GenServer.cast({__MODULE__, node}, {:update_cell, cell, value, vector_clock})
    end)
  end
end
