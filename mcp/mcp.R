# mcp.R
if (!requireNamespace("btw", quietly = TRUE)) {
  install.packages("btw")
}
btw::btw_mcp_session()
cat("✅ BTW MCP session linked\n")
