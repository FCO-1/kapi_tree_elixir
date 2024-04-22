defmodule Prueba.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      PruebaWeb.Telemetry,
      Prueba.Repo,
      {DNSCluster, query: Application.get_env(:prueba, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: Prueba.PubSub},
      # Start the Finch HTTP client for sending emails
      {Finch, name: Prueba.Finch},
      # Start a worker by calling: Prueba.Worker.start_link(arg)
      # {Prueba.Worker, arg},
      # Start to serve requests, typically the last entry
      PruebaWeb.Endpoint
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Prueba.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    PruebaWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
