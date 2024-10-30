defmodule DistributedSpreadsheet.Rest do
  use Plug.Router
  plug(Plug.Logger)
  plug(:match)

  plug(Plug.Parsers,
    parsers: [:json],
    pass: ["application/json"],
    json_decoder: Jason
  )

  plug(:dispatch)


  get "/" do
    %{"cell" => %{"column" => column, "line" => line}} = conn.params
    cell = %{column: column, line: line}
    value = DistributedSpreadsheet.Node.get_cell_value(cell)
    send_resp(conn, 200, Jason.encode!(%{"cell_value" => value}))
  end


  put "/" do
    %{"cell" => %{"column" => column, "line" => line}, "value" => value} = conn.body_params
    cell = %{column: column, line: line}
    DistributedSpreadsheet.Node.propose_cell_value(cell, value)
    send_resp(conn, 200, "Cell updated")
  end

  match _ do
    send_resp(conn, 404, "Not Found")
  end
end
