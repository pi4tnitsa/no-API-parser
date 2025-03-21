# Telegram Parser Configuration Example

# Authentication settings
auth:
  # Your Telegram phone number with country code (e.g., +12025550123)
  phone: "+12025550123"
  # Run browser in headless mode (without UI)
  headless: false
  # Optional proxy URL (e.g., socks5://user:pass@host:port)
  proxy: null

# Parser settings
parser:
  # Maximum number of posts to parse per channel
  limit: 100

# List of channels to parse
channels:
  - name: "durov"  # Channel username without @
    description: "Pavel Durov's Channel"  # Optional description
  - name: "telegram"
    description: "Official Telegram Channel"
  # You can also use channel IDs or full URLs
  - id: "1082213151"
    description: "Channel by ID"
  - url: "https://t.me/example_channel"
    description: "Channel by URL"

# Output settings
output:
  # Directory to save results
  directory: "./output"
  # Export format (json, csv, xlsx)
  format: "xlsx"