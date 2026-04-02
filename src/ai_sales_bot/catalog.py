from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path


SEARCH_STOPWORDS = {
    "а",
    "и",
    "или",
    "но",
    "только",
    "есть",
    "это",
    "что",
    "как",
    "какой",
    "какая",
    "какие",
    "каких",
    "какому",
    "какая",
    "мне",
    "могу",
    "можно",
    "нужно",
    "надо",
    "еще",
    "ещё",
    "где",
    "для",
    "по",
    "в",
    "на",
    "с",
    "из",
    "у",
    "ли",
    "бы",
    "же",
    "ну",
    "вот",
}


@dataclass(slots=True)
class ProductOption:
    label: str
    price_rub: int


@dataclass(slots=True)
class Product:
    sku: str
    category: str
    name: str
    form: str
    notes: str = ""
    tags: list[str] = field(default_factory=list)
    options: list[ProductOption] = field(default_factory=list)


class ProductCatalog:
    def __init__(self, products: list[Product] | None = None) -> None:
        self.products = products or []

    @classmethod
    def from_json(cls, path: str | Path) -> "ProductCatalog":
        catalog_path = Path(path)
        if not catalog_path.is_file():
            return cls([])

        raw = json.loads(catalog_path.read_text(encoding="utf-8"))
        products: list[Product] = []
        for item in raw.get("products", []):
            options = [
                ProductOption(
                    label=str(option.get("label", "")).strip(),
                    price_rub=int(option.get("price_rub", 0)),
                )
                for option in item.get("options", [])
            ]
            products.append(
                Product(
                    sku=str(item.get("sku", "")).strip(),
                    category=str(item.get("category", "")).strip(),
                    name=str(item.get("name", "")).strip(),
                    form=str(item.get("form", "")).strip(),
                    notes=str(item.get("notes", "")).strip(),
                    tags=[str(tag).strip() for tag in item.get("tags", []) if str(tag).strip()],
                    options=options,
                )
            )
        return cls(products)

    def _search_blob(self, product: Product) -> str:
        option_labels = " ".join(option.label for option in product.options)
        return " ".join(
            [
                product.sku,
                product.category,
                product.name,
                product.form,
                product.notes,
                " ".join(product.tags),
                option_labels,
            ]
        )

    def _normalize(self, value: str) -> str:
        return value.lower().replace("ё", "е").strip()

    def _tokenize(self, value: str) -> list[str]:
        return re.findall(r"[a-zA-Zа-яА-ЯёЁ0-9-]+", self._normalize(value))

    def _stem(self, token: str) -> str:
        token = self._normalize(token)
        endings = (
            "иями",
            "ями",
            "ами",
            "его",
            "ого",
            "ему",
            "ому",
            "ыми",
            "ими",
            "иях",
            "ах",
            "ях",
            "ия",
            "ья",
            "ие",
            "ые",
            "ой",
            "ый",
            "ий",
            "ая",
            "яя",
            "ое",
            "ее",
            "ом",
            "ем",
            "ам",
            "ям",
            "ов",
            "ев",
            "ы",
            "и",
            "а",
            "я",
            "у",
            "ю",
            "е",
            "о",
        )
        for ending in endings:
            if len(token) - len(ending) >= 4 and token.endswith(ending):
                return token[: -len(ending)]
        return token

    def search(self, query: str, *, limit: int = 10) -> list[Product]:
        needle = self._normalize(query)
        if not needle:
            return self.products[:limit]

        query_tokens = [
            token
            for token in self._tokenize(query)
            if len(token) >= 3 and token not in SEARCH_STOPWORDS
        ]
        query_stems = {self._stem(token) for token in query_tokens}
        scored: list[tuple[int, int, Product]] = []

        for product in self.products:
            haystack = self._normalize(self._search_blob(product))
            haystack_tokens = set(self._tokenize(haystack))
            haystack_stems = {self._stem(token) for token in haystack_tokens}
            score = 0
            matched_stems: set[str] = set()

            if needle in haystack:
                score += 20

            for token in query_tokens:
                token_stem = self._stem(token)
                if token in haystack_tokens:
                    score += 8
                    matched_stems.add(token_stem)
                if token_stem in haystack_stems:
                    score += 6
                    matched_stems.add(token_stem)
                if len(token_stem) >= 4 and token_stem in haystack:
                    score += 4
                    matched_stems.add(token_stem)

            if query_stems and query_stems.issubset(haystack_stems):
                score += 6

            if score > 0:
                scored.append((len(matched_stems), score, product))

        if len(query_stems) >= 2 and scored:
            max_matched_stems = max(item[0] for item in scored)
            scored = [item for item in scored if item[0] == max_matched_stems]

        scored.sort(key=lambda item: (-item[0], -item[1], item[2].name, item[2].sku))
        return [product for _, _, product in scored[:limit]]
