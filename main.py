# main.py - console interface for Steam price analyser

import config
import priceAnalys


def main() -> None:
    print("Steam price analyser (без телеграма)")
    print("====================================")
    print("Файл БД:", config.DB_PATH)
    print("Режим прокси:", config.PROXY_SELECT)
    print()

    # Инициализируем БД и папки
    priceAnalys.init_db()
    priceAnalys.load_proxies_from_file()

    print("Введите ссылку на страницу Steam (или 'q' для выхода).")
    while True:
        try:
            url = input("URL> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nВыход.")
            break

        if not url:
            continue
        if url.lower() in ("q", "quit", "exit"):
            print("Выход.")
            break

        result = priceAnalys.parsing_steam_sales(url)

        status = result.get("status")
        item_name = result.get("item_name")

        if status == "invalid_link":
            print(f"[INFO] Некорректная ссылка: {result.get('message')}")
        elif status == "dota_soon":
            print(f"[INFO] {item_name}: анализ для Dota будет добавлен позже (dota soon).")
        elif status == "blacklist":
            print(f"[INFO] {item_name}: предмет в блэклисте. Причина: {result.get('reason')}")
        elif status == "error":
            print(f"[ERROR] {item_name}: {result.get('message')}")
        elif status == "ok":
            print(
                f"[OK] {item_name}: rec_price={result['rec_price']:.4f} USD, "
                f"avg_sales={result['avg_sales']:.2f}, "
                f"tier={result['tier']}, type={result['graph_type']}"
            )
        else:
            print(f"[WARN] Неизвестный статус: {status}, результат: {result}")


if __name__ == "__main__":
    main()
