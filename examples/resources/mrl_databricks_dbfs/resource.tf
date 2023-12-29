resource "mrl_databricks_dbfs" "example" {
  adb_id      = "https://adb-12358685563655.17.azuredatabricks.net"
  token       = "dapif6546496494e8464658496f9c4219"
  local_path  = "../tools/main.go"
  content_md5 = filemd5("../tools/main.go")
}
