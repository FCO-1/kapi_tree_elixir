defmodule Prueba.InventoryBook.Context.KapitreeContext do
  @moduledoc """
    Functions for kapi tree
  """

  import Ecto.Query, warn: false
  alias Prueba.Repo

  ## Prueba.InventoryBook.Context.KapitreeContext.kapi_tree_setup("locations", "location")

  @doc """
   This function creates in different schemas the node table and the data table
   together with its view and refresh functions, the names for the data table and
   view can be different
    Please check result on your db

    In this function generate a table nodes,table data, in a materialized view,
    functions for refresh view and triggers

  ## Examples

      iex> kapi_tree_setup(schema, schema_table_data, table_name_nodes, table_name_data, view_name)
      {:ok, %Postgrex.Result{}}

  """

  def kapi_tree_setup(schema, schema_table_data, table_name_nodes, table_name_data, view_name) do
    table_created_nodes = "#{table_name_nodes}_nodes"
    path_table_node = "#{schema}.#{table_created_nodes}"
    table_created_data = "#{table_name_data}_data"
    view_created_tree = "#{table_name_data}_tree"
    Repo.transaction(fn ->
      with {:ok, table_nodes} <-  kapi_tree_structure_new_nodes(schema, table_name_nodes),
      {:ok, _bucket_transacction2} <- kapi_tree_structure_new_data(path_table_node, schema_table_data, table_name_data),
      {:ok, _bucket_transacction2} <-  kapi_tree_structure_new_tree(path_table_node, schema_table_data, view_name),
      {:ok, _bucket_transacction2} <-  kapi_tablefunc_mvw_refresh(path_table_node, schema_table_data, view_created_tree),
      {:ok, _bucket_transacction2} <-   kapi_tablefunc_updatedat(schema_table_data, table_created_data)  do
        table_nodes
      else
        {:error, error} ->
          IO.inspect error
          error
      end
    end)

  end

  @doc """
    Fuction setup  for create struct kapi tree in the same schema and same name table with subfix
    Please check result on your db

    In this function generate a table of nodes,table data, in a materialized view,
    functions for refresh view and triggers

  ## Examples

      iex>kapi_tree_setup(schema, table_name)
      {:ok, %Postgrex.Result{}}

  """

  def kapi_tree_setup(schema, table_name) do
    table_created_nodes = "#{table_name}_nodes"
    path_table_node = "#{schema}.#{table_created_nodes}"
    table_created_data = "#{table_name}_data"
    view_created_tree = "#{table_name}_tree"
    Repo.transaction(fn ->
      with {:ok, table_nodes} <-  kapi_tree_structure_new_nodes(schema, table_name),
      {:ok, _bucket_transacction2} <- kapi_tree_structure_new_data(path_table_node, schema, table_name),
      {:ok, _bucket_transacction2} <-  kapi_tree_structure_new_tree(path_table_node, schema, table_name),
      {:ok, _bucket_transacction2} <-  kapi_tablefunc_mvw_refresh(path_table_node, schema, view_created_tree),
      {:ok, _bucket_transacction2} <-   kapi_tablefunc_updatedat(schema, table_created_data)  do
        table_nodes
      else
        {:error, error} ->
          IO.inspect error
          error
      end
    end)
  end

  @doc """
  Creates struct kapi tree for table nodes.

  ## Examples

      iex> kapi_tree_structure_new_nodes("schema", "table_name")
      {:ok, %Postgrex.Result{}}

  """
  def kapi_tree_structure_new_nodes(schema, table_name) do
    Repo.query("SELECT public.kapi_tree_structure_new_nodes($1, $2);", [schema, table_name])
  end

  @doc """
  Creates struct kapi tree for table data relation of table nodes.
  This table is more for  biz logic

  ## Examples

      iex> kapi_tree_structure_new_data("schema.table_name_node", "schema", "table_name")
      {:ok, %Postgrex.Result{}}

  """
  def kapi_tree_structure_new_data(source, schema, table_name) do
    Repo.query("SELECT public.kapi_tree_structure_new_data($1, $2, $3);", [source, schema, table_name])
  end

  @doc """
  Creates material view kapi tree.

  To make consult
  ## Examples

      iex> kapi_tree_structure_new_tree("schema.table_name_node", "schema", "table_name")
      {:ok, %Postgrex.Result{}}

  """
  def kapi_tree_structure_new_tree(source, schema, table_name) do
    Repo.query("SELECT public.kapi_tree_structure_new_tree($1, $2, $3);", [source, schema, table_name])
  end

  @doc """
  Creates functions for refresh view .

  To make consult
  ## Examples

      iex> kapi_tablefunc_mvw_refresh("schema.table_name_node", "schema", "view_name")
      {:ok, %Postgrex.Result{}}

  """
  def kapi_tablefunc_mvw_refresh(source, schema, view_name) do
    Repo.query("SELECT public.kapi_tablefunc_mvw_refresh($1, $2, $3);", [source, schema, view_name])
  end

  @doc """
  Creates funcions for refresh view .

  To make consult
  ## Examples

      iex> kapi_tablefunc_updatedat("schema", "table_name_data")
      {:ok, void}

  """
  def kapi_tablefunc_updatedat(schema, table_name_data) do
    Repo.query("SELECT public.kapi_tablefunc_updatedat($1, $2);", [schema, table_name_data])
  end

end
