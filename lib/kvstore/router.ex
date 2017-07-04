defmodule KVstore.Router do
  use Plug.Router

  plug :match
  plug :dispatch

  post "/" do
    conn = fetch_query_params(conn)
    %{"key" => key, "value" => value, "ttl" => ttl} = conn.params
    KVstore.Storage.create(key, value, ttl)
    send_resp(conn, 201, "")
  end

  get "/" do
    conn = fetch_query_params(conn)
    %{"key" => key} = conn.params
    case KVstore.Storage.read(key) do
      nil ->
        send_resp(conn, 404, "")
      val ->
        send_resp(conn, 200, val)
    end
  end

  put "/" do
    conn = fetch_query_params(conn)
    %{"key" => key, "value" => value, "ttl" => ttl} = conn.params
    KVstore.Storage.update(key, value, ttl)
    send_resp(conn, 200, "")
  end

  delete "/" do
    conn = fetch_query_params(conn)
    %{"key" => key} = conn.params
    KVstore.Storage.delete(key)
    send_resp(conn, 200, "")
  end
end
