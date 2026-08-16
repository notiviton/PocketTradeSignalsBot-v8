import os
import json


print("================================")
print("Pocket Option SSID test")
print("================================")

ssid = os.getenv("POCKET_OPTION_SSID")

if not ssid:
    print("❌ POCKET_OPTION_SSID не задан")
    raise SystemExit(1)

print("✅ POCKET_OPTION_SSID найден")
print(f"SSID length: {len(ssid)}")

# Проверяем полный формат 42["auth", {...}]
if ssid.startswith('42["auth",'):
    print("✅ Обнаружен полный формат 42[\"auth\", ...]")

    try:
        payload = json.loads(ssid[2:])

        if (
            isinstance(payload, list)
            and len(payload) >= 2
            and payload[0] == "auth"
            and isinstance(payload[1], dict)
        ):
            auth = payload[1]

            print("✅ SSID имеет корректную структуру")

            print(
                f"✅ session: "
                f"{'есть' if auth.get('session') else 'нет'}"
            )

            print(
                f"✅ uid: "
                f"{'есть' if auth.get('uid') is not None else 'нет'}"
            )

            print(
                f"✅ isDemo: "
                f"{auth.get('isDemo')}"
            )

            print(
                f"✅ platform: "
                f"{auth.get('platform')}"
            )

        else:
            print("❌ Структура SSID не соответствует ожидаемой")

    except Exception as exc:
        print(f"❌ Ошибка разбора SSID: {exc}")

else:
    print("⚠️ SSID НЕ начинается с 42[\"auth\",")
    print("⚠️ Это может быть только значение session.")
    print("⚠️ Пока подключение не выполняем.")

print("================================")
print("Проверка завершена")
print("================================")
