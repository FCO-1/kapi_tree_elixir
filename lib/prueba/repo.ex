defmodule Prueba.Repo do
  use Ecto.Repo,
    otp_app: :prueba,
    adapter: Ecto.Adapters.Postgres
end
