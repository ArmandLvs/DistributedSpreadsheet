defmodule VectorClockTest do
  use ExUnit.Case
  alias DistributedSpreadsheet.VectorClock

  test "containsEntry" do
    vc1 = VectorClock.new()
    refute VectorClock.contains_entry?(vc1, 0)
    refute VectorClock.contains_entry?(vc1, 0)
    refute VectorClock.contains_entry?(vc1, 1)
    refute VectorClock.contains_entry?(vc1, 1)
  end

  test "getEntry and setEntry" do
    vc1 = VectorClock.new()
    refute VectorClock.contains_entry?(vc1, 0)

    vc1 = VectorClock.set_entry(vc1, 0, 1)

    assert VectorClock.contains_entry?(vc1, 0)
    assert VectorClock.get_entry(vc1, 0) == 1

    refute VectorClock.contains_entry?(vc1, 1)

    vc1 = VectorClock.set_entry(vc1, 1, 10)

    assert VectorClock.contains_entry?(vc1, 1)
    assert VectorClock.get_entry(vc1, 1) == 10
  end

  test "incrementEntry" do
    vc1 = VectorClock.new()
    vc1 = VectorClock.set_entry(vc1, 0, 0)

    assert VectorClock.contains_entry?(vc1, 0)
    assert VectorClock.get_entry(vc1, 0) == 0

    vc1 = VectorClock.increment_entry(vc1, 0)

    assert VectorClock.get_entry(vc1, 0) == 1
    refute VectorClock.contains_entry?(vc1, 1)

    vc1 = VectorClock.increment_entry(vc1, 1)

    assert VectorClock.get_entry(vc1, 0) == 1
    assert VectorClock.get_entry(vc1, 1) == 1

    vc1 = VectorClock.increment_entry(vc1, 1)

    assert VectorClock.get_entry(vc1, 0) == 1
    assert VectorClock.get_entry(vc1, 1) == 2

    vc1 = VectorClock.increment_entry(vc1, 2)

    assert VectorClock.get_entry(vc1, 0) == 1
    assert VectorClock.get_entry(vc1, 1) == 2
    assert VectorClock.get_entry(vc1, 2) == 1
  end

  test "max" do
    vc1 = VectorClock.new()
    vc1 = VectorClock.vmax(vc1, nil)

    refute VectorClock.contains_entry?(vc1, 0)
    assert VectorClock.get_entry(vc1, 0) == 0

    vc2 = VectorClock.new()

    vc1 = VectorClock.vmax(vc1, vc2)

    refute VectorClock.contains_entry?(vc1, 0)
    assert VectorClock.get_entry(vc1, 0) == 0

    vc2 = VectorClock.set_entry(vc2, 1, 10)
    vc1 = VectorClock.vmax(vc1, vc2)

    refute VectorClock.contains_entry?(vc1, 0)
    assert VectorClock.get_entry(vc1, 0) == 0
    assert VectorClock.get_entry(vc1, 1) == 10

    vc1 = VectorClock.set_entry(vc1, 2, 10)
    vc1 = VectorClock.vmax(vc1, vc2)

    refute VectorClock.contains_entry?(vc1, 0)
    assert VectorClock.get_entry(vc1, 0) == 0
    assert VectorClock.get_entry(vc1, 1) == 10
    assert VectorClock.get_entry(vc1, 2) == 10

    vc2 = VectorClock.set_entry(vc2, 1, 100)
    vc1 = VectorClock.vmax(vc1, vc2)

    refute VectorClock.contains_entry?(vc1, 0)
    assert VectorClock.get_entry(vc1, 0) == 0
    assert VectorClock.get_entry(vc1, 1) == 100
    assert VectorClock.get_entry(vc1, 2) == 10
  end

  test "isGreaterOrEquals" do
    vc1 = VectorClock.new()
    vc2 = VectorClock.new()

    assert VectorClock.greater_or_equals?(vc1, vc2)

    vc2 = VectorClock.increment_entry(vc2, 0)

    refute VectorClock.greater_or_equals?(vc1, vc2)

    vc1 = VectorClock.increment_entry(vc1, 0)

    assert VectorClock.greater_or_equals?(vc1, vc2)

    vc2 = VectorClock.increment_entry(vc2, 0)

    refute VectorClock.greater_or_equals?(vc1, vc2)

    vc1 = VectorClock.increment_entry(vc1, 1)

    refute VectorClock.greater_or_equals?(vc1, vc2)

    vc3 = VectorClock.new()
    vc3 = VectorClock.increment_entry(vc3, 5)
    vc3 = VectorClock.increment_entry(vc3, 6)

    refute VectorClock.greater_or_equals?(vc1, vc3)
  end

  test "equals" do
    vc1 = VectorClock.new()
    vc2 = VectorClock.new()

    assert VectorClock.equal_to?(vc1, vc2)

    vc2 = VectorClock.increment_entry(vc2, 0)

    refute VectorClock.equal_to?(vc1, vc2)

    vc1 = VectorClock.increment_entry(vc1, 0)

    assert VectorClock.equal_to?(vc1, vc2)

    vc2 = VectorClock.increment_entry(vc2, 0)

    refute VectorClock.equal_to?(vc1, vc2)

    vc1 = VectorClock.increment_entry(vc1, 1)

    refute VectorClock.equal_to?(vc1, vc2)

    vc3 = VectorClock.new()
    vc3 = VectorClock.increment_entry(vc3, 5)
    vc3 = VectorClock.increment_entry(vc3, 6)

    refute VectorClock.equal_to?(vc1, vc3)
  end
end
