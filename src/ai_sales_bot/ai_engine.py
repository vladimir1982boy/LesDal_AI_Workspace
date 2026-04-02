from __future__ import annotations

import re
from dataclasses import dataclass

from google import genai
from google.genai import types

from .catalog import Product, ProductCatalog
from .config import SalesBotConfig
from .domain import ConversationSnapshot, LeadStage


CONTACT_TRIGGERS = ("телефон", "номер", "ватсап", "whatsapp", "связаться")
ORDER_TRIGGERS = ("оформ", "заказ", "купить", "беру", "оплат")
DELIVERY_TRIGGERS = ("достав", "сдэк", "почта", "самовывоз")
PRICE_TRIGGERS = ("цена", "стоимость", "сколько стоит", "прайс")
CITY_RE = re.compile(r"\b(москва|новосибирск|алматы|усть-каменогорск|санкт-петербург|екатеринбург)\b", re.IGNORECASE)
TOKEN_STOPWORDS = {
    "есть",
    "если",
    "какая",
    "какой",
    "какие",
    "можно",
    "нужно",
    "хочу",
    "хотел",
    "хотела",
    "сколько",
    "стоит",
    "цена",
    "доставка",
    "город",
    "москва",
}
FAMILY_STEMS = {
    "пантерный": ("пантер", "партен", "партер", "патерн"),
    "красный": ("красн",),
    "королевский": ("королев",),
}
FAMILY_LABELS = {
    "пантерный": {"nom": "пантерный мухомор", "dat": "пантерному мухомору"},
    "красный": {"nom": "красный мухомор", "dat": "красному мухомору"},
    "королевский": {"nom": "королевский мухомор", "dat": "королевскому мухомору"},
}
FORM_STEMS = {
    "capsules": ("капсул",),
    "whole": ("шляп", "цел"),
    "powder": ("порош", "шаманк", "молот"),
    "caps": ("колпач", "нераскрывш"),
}
ASSORTMENT_STEMS = (
    "есть",
    "быва",
    "налич",
    "каки",
    "формат",
    "вариант",
    "объем",
    "объём",
    "предлага",
    "хочу",
    "нуж",
)
PRICE_OBJECTION_STEMS = (
    "дорог",
    "дорога",
    "дорого",
    "дорогов",
    "куса",
    "высок",
    "недеш",
)


@dataclass(slots=True)
class LeadHints:
    stage: LeadStage | None = None
    tags: list[str] | None = None
    interested_products: list[str] | None = None
    city: str | None = None


def _normalize(text: str) -> str:
    return text.lower().replace("ё", "е").strip()


def _has_any_stem(text: str, stems: tuple[str, ...]) -> bool:
    lowered = _normalize(text)
    return any(stem in lowered for stem in stems)


def _detect_family(text: str) -> str | None:
    lowered = _normalize(text)
    matches: list[tuple[int, str]] = []
    for family, stems in FAMILY_STEMS.items():
        for stem in stems:
            index = lowered.find(stem)
            if index >= 0:
                matches.append((index, family))
                break
    if not matches:
        return None
    matches.sort(key=lambda item: item[0])
    return matches[0][1]


def _detect_forms(text: str) -> list[str]:
    matched: list[str] = []
    for form_key, stems in FORM_STEMS.items():
        if _has_any_stem(text, stems):
            matched.append(form_key)
    return matched


def _format_options(product: Product) -> str:
    return ", ".join(f"{option.label} - {option.price_rub} ₽" for option in product.options)


def _format_product_for_reply(product: Product) -> str:
    return f"- {product.name}: {_format_options(product)}"


def _cheapest_option(product: Product):
    return min(product.options, key=lambda item: (item.price_rub, len(item.label), item.label))


def _match_family(product: Product, family: str) -> bool:
    if _normalize(product.category) != "griby":
        return False
    haystack = _normalize(" ".join([product.name, product.form, " ".join(product.tags)]))
    return any(stem in haystack for stem in FAMILY_STEMS.get(family, ()))


def _match_form(product: Product, form_key: str) -> bool:
    form_blob = _normalize(" ".join([product.form, " ".join(product.tags)]))
    if form_key == "capsules":
        return "капсул" in form_blob
    if form_key == "powder":
        return any(stem in form_blob for stem in FORM_STEMS["powder"])
    if form_key == "whole":
        return "цел" in form_blob or ("шляп" in form_blob and "порош" not in form_blob and "колпач" not in form_blob)
    if form_key == "caps":
        return any(stem in form_blob for stem in FORM_STEMS["caps"])
    return any(stem in form_blob for stem in FORM_STEMS.get(form_key, ()))


def _human_form_name(product: Product) -> str:
    form_blob = _normalize(" ".join([product.form, " ".join(product.tags)]))
    if "капсул" in form_blob:
        return "капсулы"
    if any(stem in form_blob for stem in FORM_STEMS["powder"]):
        return "порошок"
    if any(stem in form_blob for stem in FORM_STEMS["caps"]):
        return "колпачки"
    if "цел" in form_blob or "шляп" in form_blob:
        return "целые шляпки"
    return product.form or "формат"


def _recent_customer_messages(transcript: list[dict]) -> list[str]:
    return [
        str(row.get("text") or "").strip()
        for row in reversed(transcript[-12:])
        if str(row.get("sender_role") or "").lower() == "customer" and str(row.get("text") or "").strip()
    ]


def _is_price_objection(text: str) -> bool:
    return _has_any_stem(text, PRICE_OBJECTION_STEMS)


def _family_text(family: str | None, case: str = "nom") -> str:
    if family is None:
        return "товар"
    return FAMILY_LABELS.get(family, {}).get(case, f"{family} мухомор")


def build_catalog_rule_reply(
    *,
    catalog: ProductCatalog,
    snapshot: ConversationSnapshot,
    transcript: list[dict],
    user_message: str,
) -> str | None:
    current_family = _detect_family(user_message)
    requested_forms = _detect_forms(user_message)

    family = current_family
    if family is None:
        for text in _recent_customer_messages(transcript):
            family = _detect_family(text)
            if family is not None:
                break

    relevant_by_message = catalog.search(user_message, limit=8)
    if family is None and not requested_forms and not relevant_by_message:
        return None

    asks_assortment = _has_any_stem(user_message, ASSORTMENT_STEMS) or "?" in user_message
    if not asks_assortment and family is None and not requested_forms:
        return None

    products = list(catalog.products)
    if family is not None:
        products = [product for product in products if _match_family(product, family)]
    elif relevant_by_message:
        products = relevant_by_message

    if requested_forms:
        form_filtered = [
            product for product in products
            if any(_match_form(product, form_key) for form_key in requested_forms)
        ]
        if form_filtered:
            products = form_filtered

    if not products:
        return None

    products.sort(key=lambda item: item.name)
    family_label = _family_text(family, "nom")

    if family is not None and _is_price_objection(user_message):
        cheapest_products = sorted(
            products,
            key=lambda product: (
                _cheapest_option(product).price_rub,
                _human_form_name(product),
                product.name,
            ),
        )
        cheapest_product = cheapest_products[0]
        cheapest_variant = _cheapest_option(cheapest_product)
        lines = [f"Понимаю вас. {family_label.capitalize()} действительно не самый дешевый вариант."]
        lines.append(
            f"Самый доступный вариант по {_family_text(family, 'dat')} сейчас: {cheapest_product.name.lower()} — "
            f"{cheapest_variant.label} за {cheapest_variant.price_rub} ₽."
        )

        other_budget_lines = []
        seen_forms = {_human_form_name(cheapest_product)}
        for product in cheapest_products[1:]:
            form_name = _human_form_name(product)
            if form_name in seen_forms:
                continue
            seen_forms.add(form_name)
            option = _cheapest_option(product)
            other_budget_lines.append(f"- {form_name}: от {option.price_rub} ₽ ({option.label})")
        if other_budget_lines:
            lines.append("")
            lines.append("Если смотреть по другим форматам этого же вида:")
            lines.extend(other_budget_lines[:3])

        lines.append("")
        lines.append(f"Показать самый бюджетный вариант по {_family_text(family, 'dat')} или лучше сразу сравнить форматы?")
        return "\n".join(lines)

    if requested_forms:
        form_names = {
            "capsules": "капсулы",
            "whole": "целые шляпки",
            "powder": "порошок",
            "caps": "колпачки",
        }
        primary_form = form_names.get(requested_forms[0], "нужный формат")
        lines = [f"Да, {family_label} есть в формате «{primary_form}»:"]
        lines.extend(_format_product_for_reply(product) for product in products[:4])
        if family is not None and len({product.form for product in products}) == 1:
            other_family_products = [
                product for product in catalog.products
                if _match_family(product, family)
                and product not in products
            ]
            if other_family_products:
                other_forms = []
                seen_forms = set()
                for product in other_family_products:
                    if product.form and product.form not in seen_forms:
                        seen_forms.add(product.form)
                        other_forms.append(product.form)
                if other_forms:
                    lines.append("")
                    lines.append("Если захотите, по этому виду также есть другие форматы: " + ", ".join(other_forms) + ".")
        lines.append("")
        lines.append("Какой вариант вам удобнее?")
        return "\n".join(lines)

    if family is not None:
        lines = [f"Да, {family_label} есть в наличии. Сейчас доступны такие формы:"]
        lines.extend(_format_product_for_reply(product) for product in products[:6])
        lines.append("")
        available_forms = []
        seen_forms = set()
        for product in products:
            form_name = _human_form_name(product)
            if form_name not in seen_forms:
                seen_forms.add(form_name)
                available_forms.append(form_name)
        lines.append("Какой формат вам удобнее: " + ", ".join(available_forms) + "?")
        return "\n".join(lines)

    return None


def infer_lead_hints(text: str, catalog: ProductCatalog, snapshot: ConversationSnapshot) -> LeadHints:
    lowered = text.lower()
    tags = set(snapshot.tags)
    interested = list(snapshot.interested_products)
    stage = None
    city = None

    tokens = re.findall(r"[a-zA-Zа-яА-ЯёЁ0-9-]+", text.lower())
    matched_products = {}
    if 0 < len(tokens) <= 3:
        matched_products = {product.name: product for product in catalog.search(text, limit=5)}
    for token in tokens:
        if len(token) < 4 or token in TOKEN_STOPWORDS:
            continue
        for product in catalog.products:
            product_tokens = set(
                re.findall(
                    r"[a-zA-Zа-яА-ЯёЁ0-9-]+",
                    " ".join(
                        [
                            product.name,
                            product.category,
                            product.form,
                            product.notes,
                            " ".join(product.tags),
                        ]
                    ).lower(),
                )
            )
            if token in product_tokens:
                matched_products.setdefault(product.name, product)

    for product in matched_products.values():
        if product.name not in interested:
            interested.append(product.name)
        for tag in product.tags:
            tags.add(tag)

    if any(trigger in lowered for trigger in PRICE_TRIGGERS):
        tags.add("цена")
        stage = LeadStage.QUALIFIED
    if any(trigger in lowered for trigger in DELIVERY_TRIGGERS):
        tags.add("доставка")
        stage = LeadStage.WAITING_CONTACTS
    if any(trigger in lowered for trigger in CONTACT_TRIGGERS):
        tags.add("контакты")
        stage = LeadStage.CONTACTED
    if any(trigger in lowered for trigger in ORDER_TRIGGERS):
        tags.add("заказ")
        stage = LeadStage.PAYMENT_PENDING

    city_match = CITY_RE.search(text)
    if city_match:
        city = city_match.group(1)
        tags.add("город")

    return LeadHints(
        stage=stage,
        tags=sorted(tags) if tags else None,
        interested_products=interested or None,
        city=city,
    )


class GeminiSalesAssistant:
    def __init__(self, config: SalesBotConfig, catalog: ProductCatalog) -> None:
        self.config = config
        self.catalog = catalog
        self.client = genai.Client(api_key=config.gemini_api_key)

    def build_system_prompt(self) -> str:
        return (
            "Ты консультант LesDal по имени "
            f"{self.config.manager_name}. "
            "Ты всегда говоришь о себе в мужском роде. "
            "Ты ведешь диалог дружелюбно, уверенно и короткими сообщениями. "
            "Задавай не более одного уточняющего вопроса за ответ. "
            "Не ставь диагнозы, не обещай лечение и не замещай врача. "
            "Твоя задача: понять запрос клиента, мягко квалифицировать его, "
            "подобрать подходящие товары LesDal, ответить по формату, цене, наличию и доставке, а затем подвести к оформлению заказа или передаче менеджеру. "
            "Если клиент готов купить, собери контакты, город и способ доставки. "
            "Если вопрос спорный, сложный или медицинский, предложи подключить менеджера. "
            "Полезные материалы, файлы и чек-листы сейчас не отправляются, не обещай их клиенту. "
            "Никогда не путай красный мухомор и пантерный мухомор. "
            "Если клиент уточнил другой вид товара, сразу переключайся на этот вид, а не продолжай предыдущий. "
            "Не говори, что товара нет, закончился или доступен только в одном формате, если этого нет в релевантном каталоге. "
            "Если информации не хватает, честно скажи, что уточнишь наличие у менеджера, но не выдумывай остатки и не подменяй ассортимент."
        )

    def _format_product_line(self, product: Product) -> str:
        price_text = ", ".join(f"{option.label}: {option.price_rub} ₽" for option in product.options[:6])
        form_text = f", формат: {product.form}" if product.form else ""
        return f"- {product.name}{form_text}. Цены: {price_text}"

    def _resolve_catalog_products(
        self,
        snapshot: ConversationSnapshot,
        user_message: str,
        transcript: list[dict],
    ) -> tuple[str, list[Product]]:
        direct_matches = self.catalog.search(user_message, limit=6)
        if direct_matches:
            return "Релевантные позиции по текущему сообщению клиента:", direct_matches

        for text in _recent_customer_messages(transcript):
            matches = self.catalog.search(text, limit=6)
            if matches:
                return "Релевантные позиции по последним сообщениям клиента:", matches

        matched_from_snapshot: list[Product] = []
        seen_skus = set()
        for name in reversed(snapshot.interested_products[-6:]):
            for product in self.catalog.search(name, limit=6):
                if product.sku in seen_skus:
                    continue
                seen_skus.add(product.sku)
                matched_from_snapshot.append(product)
        if matched_from_snapshot:
            return "Позиции из накопленного интереса клиента:", matched_from_snapshot[:6]

        return "Каталог не сузился автоматически, сначала уточни вид товара или формат.", []

    def _catalog_context(
        self,
        snapshot: ConversationSnapshot,
        user_message: str,
        transcript: list[dict],
    ) -> str:
        title, products = self._resolve_catalog_products(snapshot, user_message, transcript)
        if not products:
            return title

        lines = [title]
        for product in products:
            lines.append(self._format_product_line(product))
        return "\n".join(lines)

    def _history_text(self, transcript: list[dict]) -> str:
        lines = []
        for row in reversed(transcript[-12:]):
            sender = row.get("sender_name") or row.get("sender_role") or "unknown"
            text = str(row.get("text") or "").strip()
            if text:
                lines.append(f"{sender}: {text}")
        return "\n".join(lines)

    def generate_reply(
        self,
        *,
        snapshot: ConversationSnapshot,
        transcript: list[dict],
        user_message: str,
    ) -> str:
        rule_reply = build_catalog_rule_reply(
            catalog=self.catalog,
            snapshot=snapshot,
            transcript=transcript,
            user_message=user_message,
        )
        if rule_reply:
            return rule_reply

        catalog_context = self._catalog_context(snapshot, user_message, transcript)
        prompt = (
            f"Стадия лида: {snapshot.stage.value}\n"
            f"Режим: {snapshot.mode.value}\n"
            f"Теги: {', '.join(snapshot.tags) if snapshot.tags else '-'}\n"
            f"{catalog_context}\n\n"
            f"Последние сообщения:\n{self._history_text(transcript)}\n\n"
            f"Последнее сообщение клиента: {user_message}\n\n"
            "Сформируй один ответ для клиента на русском языке. "
            "Отвечай только по тем позициям, которые релевантны текущему запросу и перечислены выше. "
            "Если клиент спрашивает о конкретном виде, сначала дай ответ именно по этому виду. "
            "Если уместно, предложи 2-4 варианта товара или следующий шаг. "
            "Не используй markdown-разметку, кроме коротких списков с дефисом."
        )
        response = self.client.models.generate_content(
            model=self.config.gemini_model,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=self.build_system_prompt(),
                temperature=0.7,
            ),
        )
        return (response.text or "").strip()
