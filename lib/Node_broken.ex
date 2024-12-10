defmodule DistributedSpreadsheet.Nodebroken do
  use GenServer
  require Logger

  defmodule State do
    @enforce_keys [:cells, :vector_clock, :msgBag, :msgSeq, :delivered]
    defstruct cells: %{}, vector_clock: DistributedSpreadsheet.VectorClock.new(), msgBag: [], msgSeq: 0, delivered: MapSet.new()

    @type t() :: %__MODULE__{
            cells: %{optional(any()) => any()},
            vector_clock: DistributedSpreadsheet.VectorClock.t(),
            msgBag: [{any(), DistributedSpreadsheet.VectorClock.t()}],
            msgSeq: 0,
            delivered: MapSet.t()
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
    {:ok, %State{cells: %{}, vector_clock: DistributedSpreadsheet.VectorClock.new(), msgBag: [], msgSeq: 0, delivered: MapSet.new()}}
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
  def handle_cast({:propose_cell_value, cell, value}, %State{cells: cells, vector_clock: vc, msgSeq: seq} = state) do
    updated_vc = DistributedSpreadsheet.VectorClock.increment_entry(vc, node())
    updated_msgSeq = seq + 1
    new_state = %State{state | cells: Map.put(cells, cell, value), vector_clock: updated_vc, msgSeq: updated_msgSeq}
    reliable_broadcast_cell_update(cell, value, updated_vc, node(), updated_msgSeq, %State{delivered: delivered} = state)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:update_cell, node, msgSeq, cell, value, vector_clock}, %State{cells: cells, vector_clock: vc, msgBag: msgBag, delivered: delivered} = state) do
    reliable_broadcast_cell_update(cell, value, vector_clock, node, msgSeq, %State{delivered: delivered} = state)
    if node != node() do
      new_msgBag = [{cell, value, vector_clock} | msgBag]
      # Process messages that can be delivered
      {delivered, new_msgBag} = Enum.reduce(new_msgBag, {[], []}, fn {cell, value, msg_vc}, {delivered, remaining_msgBag} ->
        if DistributedSpreadsheet.VectorClock.greater_or_equals?(vc, msg_vc) do
          # If the message can be delivered, apply it
          new_cells = Map.put(cells, cell, value)
          new_vc = DistributedSpreadsheet.VectorClock.increment_entry(vc, node)
          delivered = [{cell, value, new_vc} | delivered]
          {delivered, remaining_msgBag}
        else
          # Keep the message in the bag if it can't be delivered
          {delivered, [{cell, value, msg_vc} | remaining_msgBag]}
        end
      end)
      ### To check here, the implementation seems not good, cause we deliver the message after the if condition even though it's not checked.

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

  defp reliable_broadcast_cell_update(cell, value, vector_clock,node, msgSeq, %State{delivered: delivered} = state) do
    current_node = node()
    Logger.info("#{node()} started broadcasting msg #{msgSeq} of Node #{node}.")
    updated_delivered = MapSet.put(delivered, {node, msgSeq})
    %State{state | delivered: updated_delivered}

    if not MapSet.member?(delivered, {node, msgSeq}) do
      Node.list()
      |> Enum.each(fn member ->
        if member != current_node do
          GenServer.cast(
            {__MODULE__, member},
            {:update_cell, node, msgSeq, cell, value, vector_clock}
          )
        end
      end)
      new_vc = DistributedSpreadsheet.VectorClock.increment_entry(vector_clock, node())
      %State{state | vector_clock: new_vc}
    else
      state
    end
    Logger.info("#{node()} finished broadcasting msg #{msgSeq} of Node #{node}.")
  end

end
