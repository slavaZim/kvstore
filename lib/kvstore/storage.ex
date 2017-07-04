defmodule KVstore.Storage do
  use GenServer

  @timer_tick 100

  def start_link() do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def create(key, value, ttl) do
    GenServer.cast(__MODULE__, {:create, key, value, String.to_integer(ttl)})
  end

  def update(key, value, ttl) do
    GenServer.cast(__MODULE__, {:update, key, value, String.to_integer(ttl)})
  end

  def read(key) do
    GenServer.call(__MODULE__, {:read, key})
  end

  def delete(key) do
    GenServer.cast(__MODULE__, {:delete, key})
  end

  def init(:ok) do
    Process.flag(:trap_exit, true)
    :dets.open_file(:storage, [type: :set])
    :dets.open_file(:ttl, [type: :bag])
    cache_storage = :ets.new(:cache_storage, [:set, :public])
    cache_ttl = :ets.new(:cache_ttl, [:bag, :public])
    :ets.from_dets(cache_storage, :storage)
    :ets.from_dets(cache_ttl, :ttl)
    t = now
    cleanup(cache_storage, cache_ttl, t)
    schedule_timer
    {:ok, {cache_storage, cache_ttl, t}}
  end

  def handle_call({:read, key}, _from, {cache_storage, cache_ttl, lower_bound}) do
    resp =
      case :ets.lookup(cache_storage, key) do
      [{_key, val, expires_at}] ->
        if now < expires_at do
          val
        else
          nil
        end
      [] ->
        nil
      end
    {:reply, resp, {cache_storage, cache_ttl, lower_bound}}
  end

  def handle_cast({:create, key, value, ttl}, {cache_storage, cache_ttl, lower_bound}) do
    t = time(ttl)
    if :ets.insert_new(cache_storage, {key, value, t}) do
      :ets.insert(cache_ttl, {t, key})
    end
    {:noreply, {cache_storage, cache_ttl, lower_bound}}
  end

  def handle_cast({:update, key, value, ttl}, {cache_storage, cache_ttl, lower_bound}) do
    t = time(ttl)
    case :ets.lookup(cache_storage, key) do
      [{key, _, expires_at}] ->
        if now < expires_at do
          :ets.insert(cache_storage, {key, value, t})
          :ets.insert(cache_ttl, {t, key})
        end
      _ ->
        nil
    end
    {:noreply, {cache_storage, cache_ttl, lower_bound}}
  end

  def handle_cast({:delete, key}, {cache_storage, cache_ttl, lower_bound}) do
    :ets.delete(cache_storage, key)
    {:noreply, {cache_storage, cache_ttl, lower_bound}}
  end

  def handle_info(:expire, {cache_storage, cache_ttl, lower_bound}) do
    t = now
    process_expiration(cache_storage, cache_ttl, lower_bound+1, t)
    schedule_timer
    {:noreply, {cache_storage, cache_ttl, t}}
  end

  def handle_info({:EXIT, _pid, :normal}, {cache_storage, cache_ttl, lower_bound}) do
    {:noreply, {cache_storage, cache_ttl, lower_bound}}
  end

  def terminate(_reason, {cache_storage, cache_ttl, _lower_bound}) do
    :ets.to_dets(cache_storage, :storage)
    :ets.to_dets(cache_ttl, :ttl)
    :ets.delete(cache_storage)
    :ets.delete(cache_ttl)
    :dets.close(:storage)
    :dets.close(:ttl)
  end

  defp schedule_timer do
    Process.send_after(self(), :expire, @timer_tick)
  end

  defp process_expiration(cache_storage, cache_ttl, lower_bound, t) do
    spawn_link fn ->
      collect_keys_and_clean_bag(cache_ttl, lower_bound, t)
      |> Enum.each(&expire(cache_storage, &1, t))
    end
  end

  defp collect_keys_and_clean_bag(cache_ttl, lower_bound, t) do
    Enum.reduce((lower_bound..t), MapSet.new([]), &MapSet.union(bag_walk(cache_ttl, &1), &2))
  end

  defp bag_walk(cache_ttl, t) do
    keys = :ets.select(cache_ttl, [{{t, :"$1"}, [], [:"$1"]}]) |> MapSet.new
    :ets.select_delete(cache_ttl, [{{t, :"_"}, [], [true]}])
    keys
  end

  defp expire(cache_storage, key, t) do
    :ets.select_delete(cache_storage, [{{key, :"_", :"$1"}, [{:"=<",:"$1", t}], [true]}])
  end

  defp cleanup(cache_store, cache_ttl, t) do
    :ets.select_delete(cache_store, [{{:"_", :"_", :"$1"}, [{:"=<", :"$1", t}], [true]}])
    :ets.select_delete(cache_ttl, [{{:"$1", :"_"}, [{:"=<", :"$1", t}], [true]}])
  end

  defp now do
    :os.system_time(:millisecond)
  end

  defp time(milliseconds) do
    now + milliseconds
  end
end
