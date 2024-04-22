defmodule MvpTecnopack.Repo.Migrations.ExtencionAuthSetup do
  use Ecto.Migration

  def change do
    execute """
      CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    """,""
    execute """
      CREATE EXTENSION IF NOT EXISTS pgcrypto;
    """,""
    execute """
      CREATE EXTENSION IF NOT EXISTS citext;
    """,""
  end
end
