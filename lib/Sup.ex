defmodule DistributedSpreadsheet.Sup do
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    # We start the HTTP server if the DSN_PORT environment variable is set.
    http =
      case System.get_env("DSN_PORT") do
        nil ->
          []

        http_port ->
          http_opts = [port: String.to_integer(http_port)]
          [{Plug.Cowboy, scheme: :http, plug: DistributedSpreadsheet.Rest, options: http_opts}]
      end

    # Define the children processes that the supervisor will start and manage
    children = [
      %{id: DistributedSpreadsheet.Node, start: {DistributedSpreadsheet.Node, :start_link, []}}
       ]
       ++ http

    # Set the supervision strategy as :one_for_all
    Supervisor.init(children, strategy: :one_for_all)
  end
end
