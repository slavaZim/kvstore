defmodule KVstoreTest do
  use ExUnit.Case
  use Plug.Test

  @opts KVstore.Router.init([])

  setup_all do
    File.rm("storage")
    File.rm("ttl")
    :ok
  end

  setup do
    Application.stop(:kvstore)
    File.rm("storage")
    File.rm("ttl")
    :ok = Application.start(:kvstore)
  end

  test "returns 404 when not found" do
    conn = conn(:get, "/", %{"key" => "somekey"})
    conn = KVstore.Router.call(conn, @opts)
    assert conn.status == 404
  end

  test "assigns a key and returns by request" do
    post = conn(:post, "/", %{"key" => "somekey", "value" => "somevalue", "ttl" => "1000"})
    post = KVstore.Router.call(post, @opts)
    get = conn(:get, "/", %{"key" => "somekey"})
    get = KVstore.Router.call(get, @opts)
    assert post.status == 201
    assert get.resp_body == "somevalue"
  end

  test "updates a key" do
    KVstore.Storage.create("somekey", "val", "3000")
    update = conn(:put, "/", %{"key" => "somekey", "value" => "anotherval", "ttl" => "2000"})
    KVstore.Router.call(update, @opts)
    assert get_val("somekey") == "anotherval"
  end

  test "deletes a key" do
    KVstore.Storage.create("delete", "val", "3000")
    delete = conn(:delete, "/", %{"key" => "delete"})
    KVstore.Router.call(delete, @opts)
    assert get_val("delete") == nil
  end

  test "expires a key" do
    KVstore.Storage.create("expire", "val", "500")
    KVstore.Storage.create("expire2", "val", "500")
    :timer.sleep(1100)
    assert get_val("expire") == nil
    assert get_val("expire2") == nil
    KVstore.Storage.create("expire", "val", "1000")
    :timer.sleep(500)
    KVstore.Storage.update("expire", "val", "1000")
    :timer.sleep(600)
    refute get_val("expire") == nil
    :timer.sleep(1500)
    assert get_val("expire") == nil
  end

  test "recreate key works as expected" do
    KVstore.Storage.create("key", "val", "900")
    :timer.sleep(500)
    KVstore.Storage.delete("key")
    :timer.sleep(100)
    KVstore.Storage.create("key", "anotherval", "1700")
    :timer.sleep(1400)
    assert get_val("key") == "anotherval"
    :timer.sleep(1400)
    assert get_val("key") == nil
  end

  test "application stop and start" do
    KVstore.Storage.create("key", "persisted", "5000")
    Application.stop(:kvstore)
    Application.start(:kvstore)
    assert get_val("key") == "persisted"
  end

  defp get_val(key) do
    :sys.get_state(KVstore.Storage) |> elem(0) |> :ets.lookup(key) |> Enum.at(0) |> elem(1)
  rescue _ ->
    nil
  end
end
