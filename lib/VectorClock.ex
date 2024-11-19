defmodule DistributedSpreadsheet.VectorClock do
  @moduledoc """
  A module that implements a vector clock using a map.

  A vector clock is used to maintain causal relationships between events in distributed systems.
  """

  @enforce_keys [:vector]
  defstruct vector: %{}

  @type t() :: %__MODULE__{vector: %{optional(atom() | String.t() | integer()) => integer()}}
  @doc """
  Creates a new empty vector clock.
  """
  def new do
    %DistributedSpreadsheet.VectorClock{vector: %{}}
  end

  @doc """
  Creates a new vector clock by cloning an existing vector clock.
  """
  def clone(%DistributedSpreadsheet.VectorClock{vector: original_vector}) do
    %DistributedSpreadsheet.VectorClock{vector: Map.new(original_vector)}
  end

  @doc """
  Checks the invariant of the vector clock: all values are greater than or equal to 0.
  """
  def invariant?(%DistributedSpreadsheet.VectorClock{vector: vector}) do
    Enum.all?(vector, fn {_key, value} -> value >= 0 end)
  end

  @doc """
  Gets the map representing the vector clock.
  """
  def get_vector(%DistributedSpreadsheet.VectorClock{vector: vector}), do: vector

  @doc """
  Gets the value of the clock for a given process key.
  Returns 0 if the key does not exist.
  """
  def get_entry(%DistributedSpreadsheet.VectorClock{vector: vector}, key) do
    Map.get(vector, key, 0)
  end

  @doc """
  Checks if the vector clock contains an entry for the given key.
  """
  def contains_entry?(%DistributedSpreadsheet.VectorClock{vector: vector}, key) do
    Map.has_key?(vector, key)
  end

  @doc """
  Sets the value of the clock for a given process key.
  If the key does not exist, it is inserted with the specified value.
  Raises an error if the key is negative.
  """
  def set_entry(%DistributedSpreadsheet.VectorClock{vector: _vector} = _vc, key, _value) when key < 0 do
    raise ArgumentError, "Invalid process identifier (#{key})"
  end

  def set_entry(%DistributedSpreadsheet.VectorClock{vector: vector} = vc, key, value) do
    %DistributedSpreadsheet.VectorClock{vc | vector: Map.put(vector, key, value)}
  end

  @doc """
  Increments the clock value for a given process key.
  If the key does not exist, it is inserted with a value of 1.
  """
  def increment_entry(%DistributedSpreadsheet.VectorClock{vector: _vector} = _vc, key) when key < 0 do
    raise ArgumentError, "Invalid process identifier (#{key})"
  end

  def increment_entry(%DistributedSpreadsheet.VectorClock{vector: _vector} = vc, key) do
    new_value = get_entry(vc, key) + 1
    set_entry(vc, key, new_value)
  end

  @doc """
  Merges the current vector clock with another vector clock, taking the maximum values for each key.
  If a key exists in one clock but not the other, the existing value is kept.
  """
  def vmax(vc1, nil), do: vc1
  def vmax(nil, vc2), do: vc2
  def vmax(%DistributedSpreadsheet.VectorClock{vector: vector1} = vc1, %DistributedSpreadsheet.VectorClock{vector: vector2}) do
    keys = Map.keys(vector1) ++ Map.keys(vector2)

    merged_vector =
      Enum.reduce(keys, %{}, fn key, acc ->
        max_value = max(get_entry(vc1, key), get_entry(%DistributedSpreadsheet.VectorClock{vector: vector2}, key))
        Map.put(acc, key, max_value)
      end)

    %DistributedSpreadsheet.VectorClock{vector: merged_vector}
  end

  @doc """
  Checks if the current vector clock is greater than or equal to another vector clock.
  """
  def greater_or_equals?(%DistributedSpreadsheet.VectorClock{vector: vector1} = vc1, %DistributedSpreadsheet.VectorClock{vector: vector2}) do
    keys = Map.keys(vector1) ++ Map.keys(vector2)

    Enum.all?(keys, fn key ->
      get_entry(vc1, key) >= get_entry(%DistributedSpreadsheet.VectorClock{vector: vector2}, key)
    end)
  end

  @doc """
  Checks if the current vector clock is equal to another vector clock.
  """
  def equal_to?(%DistributedSpreadsheet.VectorClock{vector: vector1} = vc1, %DistributedSpreadsheet.VectorClock{vector: vector2}) do
    keys = Map.keys(vector1) ++ Map.keys(vector2)

    Enum.all?(keys, fn key ->
      get_entry(vc1, key) == get_entry(%DistributedSpreadsheet.VectorClock{vector: vector2}, key)
    end)
  end

  @doc """
  Converts the vector clock to a string for display.
  """
  def to_string(%DistributedSpreadsheet.VectorClock{vector: vector}) do
    inspect(vector)
  end
end
