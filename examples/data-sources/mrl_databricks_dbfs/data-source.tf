data "mrl_databricks_dbfs" "example" {
  adb_id    = "https://adb-12358685563655.17.azuredatabricks.net"
  token     = "dapif6546496494e8464658496f9c4219"
  root_path = "/FileStore/jars/init-libs"
}
