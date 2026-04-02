from __future__ import annotations

import unittest

from src.ai_sales_bot.catalog import Product, ProductCatalog, ProductOption


class ProductCatalogSearchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = ProductCatalog(
            [
                Product(
                    sku="panther-caps",
                    category="griby",
                    name="Пантерный мухомор в капсулах 2025",
                    form="капсулы",
                    tags=["пантерный", "пантерный мухомор", "мухомор", "капсулы"],
                    options=[ProductOption(label="32 шт", price_rub=1200)],
                ),
                Product(
                    sku="panther-whole",
                    category="griby",
                    name="Шляпки пантерного мухомора целые 2025",
                    form="целые шляпки",
                    tags=["пантерный", "пантерный мухомор", "мухомор", "шляпки"],
                    options=[ProductOption(label="25 г", price_rub=2000)],
                ),
                Product(
                    sku="red-caps",
                    category="griby",
                    name="Красный мухомор в капсулах 2025",
                    form="капсулы",
                    tags=["красный", "красный мухомор", "мухомор", "капсулы"],
                    options=[ProductOption(label="32 шт", price_rub=900)],
                ),
                Product(
                    sku="red-tea",
                    category="chinese_tea",
                    name="Да Хун Пао",
                    form="китайский чай",
                    notes="Большой красный халат.",
                    tags=["чай"],
                    options=[ProductOption(label="50 г", price_rub=500)],
                ),
            ]
        )

    def test_search_handles_russian_word_forms_for_panther(self) -> None:
        names = [product.name for product in self.catalog.search("а пантерный?", limit=5)]

        self.assertIn("Пантерный мухомор в капсулах 2025", names)
        self.assertIn("Шляпки пантерного мухомора целые 2025", names)
        self.assertNotIn("Красный мухомор в капсулах 2025", names)

    def test_search_prefers_products_matching_all_meaningful_tokens(self) -> None:
        names = [product.name for product in self.catalog.search("красный в капсулах", limit=5)]

        self.assertEqual(names, ["Красный мухомор в капсулах 2025"])


if __name__ == "__main__":
    unittest.main()
