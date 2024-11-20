defmodule DistributedSpreadsheet.Node do
  use GenServer
  require Logger

  defmodule State do
    @enforce_keys [:cells, :vector_clock, :msgBag]
    defstruct cells: %{}, vector_clock: DistributedSpreadsheet.VectorClock.new(), msgBag: []

    @type t() :: %__MODULE__{
            cells: %{optional(any()) => any()},
            vector_clock: DistributedSpreadsheet.VectorClock.t(),
            msgBag: [{any(), DistributedSpreadsheet.VectorClock.t()}]
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
    {:ok, %State{cells: %{}, vector_clock: DistributedSpreadsheet.VectorClock.new(), msgBag: []}}
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
  def handle_cast({:update_cell, node, cell, value, vector_clock}, %State{cells: cells, vector_clock: vc, msgBag: msgBag} = state) do
    # Only handle messages not from this node
    if node != node() do
      new_msgBag = [{cell, value, vector_clock} | msgBag]
      # Process messages that can be delivered
      {delivered, new_msgBag} = Enum.reduce(new_msgBag, {[], []}, fn {cell, value, msg_vc}, {delivered, remaining_msgBag} ->
        if DistributedSpreadsheet.VectorClock.greater_or_equals?(vc, msg_vc) do
          # If the message can be delivered, apply it
          new_cells = Map.put(cells, cell, value)
          new_vc = DistributedSpreadsheet.VectorClock.increment_entry(vc, node())
          delivered = [{cell, value, new_vc} | delivered]
          {delivered, remaining_msgBag}
        else
          # Keep the message in the bag if it can't be delivered
          {delivered, [{cell, value, msg_vc} | remaining_msgBag]}
        end
      end)

      # After processing messages, we update the state
      updated_vc = DistributedSpreadsheet.VectorClock.vmax(vector_clock, vc)
      new_state = %State{state | cells: Map.put(cells, cell, value), vector_clock: updated_vc, msgBag: new_msgBag}
      {:noreply, new_state}
    else
      # Return without changes if the message is from the same node
      {:noreply, state}
    end
  end

  ### Private Functions

  defp broadcast_cell_update(cell, value, vector_clock) do
    members = Node.list()
    Enum.each(members, fn node ->
      GenServer.cast({__MODULE__, node}, {:update_cell, node(), cell, value, vector_clock})
    end)
  end
end
