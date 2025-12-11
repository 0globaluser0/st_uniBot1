# test_lis_api.py
"""
Консольный тестер для lis_api.process_item.

Позволяет ввести:
  - название предмета;
  - QUANTITY_MAX_ALLOWED;
  - SUM_MAX_ALLOWED;

и запускает полный цикл фильтрации лотов LIS.
"""

import sys

import priceAnalys
from lis_api import process_item


def main() -> None:
    print("=== Тестер LIS API (market/search) ===")
    print("БД и директории инициализируются (init_db)...")
    priceAnalys.init_db()
    print("Готово.\n")

    print("Введите название предмета/скина, либо 'q' для выхода.")

    while True:
        try:
            name = input("Имя предмета> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nВыход.")
            break

        if not name:
            continue

        if name.lower() in ("q", "quit", "exit"):
            print("Выход.")
            break

        # QUANTITY_MAX_ALLOWED
        try:
            qty_str = input("QUANTITY_MAX_ALLOWED (макс. кол-во лотов)> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nВыход.")
            break

        if not qty_str:
            print("Пустое значение, попробуйте ещё раз.\n")
            continue

        try:
            quantity_max = int(qty_str)
        except ValueError:
            print("Некорректное целое число для QUANTITY_MAX_ALLOWED.\n")
            continue

        # SUM_MAX_ALLOWED
        try:
            sum_str = input("SUM_MAX_ALLOWED (макс. сумма, USD)> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nВыход.")
            break

        if not sum_str:
            print("Пустое значение, попробуйте ещё раз.\n")
            continue

        sum_str = sum_str.replace(",", ".")
        try:
            sum_max = float(sum_str)
        except ValueError:
            print("Некорректное число для SUM_MAX_ALLOWED.\n")
            continue

        try:
            result = process_item(name, quantity_max, sum_max)
        except Exception as e:
            print(f"[ERROR] Ошибка при обработке '{name}': {e}\n")
            continue

        print(
            f"[OK] {name}: после фильтра по прибыли осталось "
            f"{result['total_filtered_quantity']} лотов на сумму "
            f"{result['total_filtered_sum']:.2f} USD."
        )
        print(
            f"    После ограничения по количеству/сумме выбрано "
            f"{result['final_quantity']} лотов на сумму "
            f"{result['final_sum']:.2f} USD."
        )

        files = result.get("files", {})
        print("    Файлы сохранены:")
        print(f"      0-й этап: {files.get('stage0')}")
        print(f"      1-й этап: {files.get('stage1')}")
        print(f"      2-й этап: {files.get('stage2')}")
        print()

    print("Готово.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
        sys.exit(0)
