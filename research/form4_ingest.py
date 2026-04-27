"""Parse SEC Form 4 XML filings into purchase transaction records."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any
from xml.etree import ElementTree


EXECUTIVE_TITLE_KEYWORDS = (
    "CEO",
    "CHIEF EXECUTIVE",
    "CFO",
    "CHIEF FINANCIAL",
    "COO",
    "CHIEF OPERATING",
    "PRESIDENT",
    "CHAIR",
    "CHAIRMAN",
    "CHAIRWOMAN",
    "VP",
    "VICE PRESIDENT",
    "GENERAL COUNSEL",
    "GC",
)


def parse_form4_xml(
    xml_text: str,
    *,
    accepted_at: str | None = None,
    accession: str | None = None,
    cik: str | None = None,
    ticker: str | None = None,
) -> list[dict[str, Any]]:
    """Return eligible non-derivative Form 4 purchase transactions.

    Only non-derivative transactions with transaction code ``P`` and
    acquired/disposed code ``A`` are emitted. Caller-provided filing metadata
    is retained verbatim on every record.
    """

    root = ElementTree.fromstring(xml_text)
    issuer = _issuer_metadata(root)
    owners = _reporting_owners(root)
    is_amendment = _text(root, "documentType").upper() == "4/A"

    records: list[dict[str, Any]] = []
    for transaction in _children(_child(root, "nonDerivativeTable"), "nonDerivativeTransaction"):
        if _text(transaction, "transactionCode").upper() != "P":
            continue
        if _text(transaction, "transactionAcquiredDisposedCode").upper() != "A":
            continue

        shares = _decimal(_text(transaction, "transactionShares"))
        price = _decimal(_text(transaction, "transactionPricePerShare"))
        purchase_value = shares * price if shares is not None and price is not None else None

        record = {
            "accepted_at": accepted_at,
            "accession": accession,
            "cik": cik,
            "ticker": ticker,
            "amendment": is_amendment,
            "issuer_cik": issuer.get("issuer_cik"),
            "issuer_name": issuer.get("issuer_name"),
            "issuer_trading_symbol": issuer.get("issuer_trading_symbol"),
            "security_title": _text(transaction, "securityTitle") or None,
            "transaction_date": _text(transaction, "transactionDate") or None,
            "transaction_code": "P",
            "acquired_disposed_code": "A",
            "shares": shares,
            "price_per_share": price,
            "purchase_value": purchase_value,
            "reporting_owners": owners,
            "eligible_insider": _eligible_insider(owners),
        }
        records.append(record)

    return records


def _issuer_metadata(root: ElementTree.Element) -> dict[str, str | None]:
    issuer = _child(root, "issuer")
    return {
        "issuer_cik": _text(issuer, "issuerCik") or None,
        "issuer_name": _text(issuer, "issuerName") or None,
        "issuer_trading_symbol": _text(issuer, "issuerTradingSymbol") or None,
    }


def _reporting_owners(root: ElementTree.Element) -> list[dict[str, Any]]:
    owners = []
    for owner in _children(root, "reportingOwner"):
        relationship = _child(owner, "reportingOwnerRelationship")
        owner_record = {
            "cik": _text(owner, "rptOwnerCik") or None,
            "name": _text(owner, "rptOwnerName") or None,
            "is_director": _bool_text(_text(relationship, "isDirector")),
            "is_officer": _bool_text(_text(relationship, "isOfficer")),
            "is_ten_percent_owner": _bool_text(_text(relationship, "isTenPercentOwner")),
            "is_other": _bool_text(_text(relationship, "isOther")),
            "officer_title": _text(relationship, "officerTitle") or None,
        }
        owners.append(owner_record)
    return owners


def _eligible_insider(owners: list[dict[str, Any]]) -> bool | None:
    if not owners:
        return None

    saw_relationship_metadata = False
    for owner in owners:
        relationship_flags = (
            owner["is_director"],
            owner["is_officer"],
            owner["is_ten_percent_owner"],
            owner["is_other"],
        )
        if any(flag is not None for flag in relationship_flags) or owner["officer_title"]:
            saw_relationship_metadata = True

        if owner["is_director"] or owner["is_officer"]:
            return True

        title = (owner["officer_title"] or "").upper()
        if any(keyword in title for keyword in EXECUTIVE_TITLE_KEYWORDS):
            return True

    return False if saw_relationship_metadata else None


def _decimal(value: str) -> Decimal | None:
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def _bool_text(value: str) -> bool | None:
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    return None


def _text(element: ElementTree.Element | None, tag_name: str) -> str:
    child = _child(element, tag_name)
    if child is None:
        return ""

    value = child.text.strip() if child.text else ""
    if value:
        return value

    value_child = _child(child, "value")
    if value_child is None or value_child.text is None:
        return ""
    return value_child.text.strip()


def _child(element: ElementTree.Element | None, tag_name: str) -> ElementTree.Element | None:
    if element is None:
        return None
    for candidate in element.iter():
        if _local_name(candidate.tag) == tag_name:
            return candidate
    return None


def _children(element: ElementTree.Element | None, tag_name: str) -> list[ElementTree.Element]:
    if element is None:
        return []
    return [child for child in list(element) if _local_name(child.tag) == tag_name]


def _local_name(tag: str) -> str:
    return tag.rsplit("}", maxsplit=1)[-1]
