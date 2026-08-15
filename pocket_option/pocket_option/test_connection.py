import os

print("================================")
print("Pocket Option connection test")
print("================================")

ssid = os.getenv("POCKET_OPTION_SSID")

if not ssid:
    print("❌ POCKET_OPTION_SSID не задан")
else:
    print("✅ POCKET_OPTION_SSID найден")
    print(f"SSID length: {len(ssid)}")
