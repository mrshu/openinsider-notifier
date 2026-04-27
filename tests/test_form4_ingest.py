from decimal import Decimal
import unittest

from research.form4_ingest import parse_form4_xml


BASE_FORM4 = """\
<ownershipDocument>
  <documentType>4</documentType>
  <issuer>
    <issuerCik>0001234567</issuerCik>
    <issuerName>Example Corp</issuerName>
    <issuerTradingSymbol>EXM</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0007654321</rptOwnerCik>
      <rptOwnerName>Jane Buyer</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>0</isDirector>
      <isOfficer>1</isOfficer>
      <isTenPercentOwner>0</isTenPercentOwner>
      <isOther>0</isOther>
      <officerTitle>Chief Financial Officer</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-04-24</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>125.5</value></transactionShares>
        <transactionPricePerShare><value>10.20</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>10</value></transactionShares>
        <transactionPricePerShare><value>99</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <derivativeTable>
    <derivativeTransaction>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>999</value></transactionShares>
        <transactionPricePerShare><value>1</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </derivativeTransaction>
  </derivativeTable>
</ownershipDocument>
"""


class Form4IngestTests(unittest.TestCase):
    def test_parses_only_non_derivative_acquired_purchases(self):
        records = parse_form4_xml(
            BASE_FORM4,
            accepted_at="2026-04-25T10:11:12Z",
            accession="0001234567-26-000001",
            cik="caller-cik",
            ticker="CALL",
        )

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["accepted_at"], "2026-04-25T10:11:12Z")
        self.assertEqual(record["accession"], "0001234567-26-000001")
        self.assertEqual(record["cik"], "caller-cik")
        self.assertEqual(record["ticker"], "CALL")
        self.assertFalse(record["amendment"])
        self.assertEqual(record["issuer_cik"], "0001234567")
        self.assertEqual(record["issuer_trading_symbol"], "EXM")
        self.assertEqual(record["transaction_date"], "2026-04-24")
        self.assertEqual(record["security_title"], "Common Stock")
        self.assertEqual(record["shares"], Decimal("125.5"))
        self.assertEqual(record["price_per_share"], Decimal("10.20"))
        self.assertEqual(record["purchase_value"], Decimal("1280.100"))
        self.assertTrue(record["eligible_insider"])
        self.assertEqual(record["reporting_owners"][0]["name"], "Jane Buyer")

    def test_marks_amendments_without_deduping(self):
        amended_xml = BASE_FORM4.replace(
            "<documentType>4</documentType>",
            "<documentType>4/A</documentType>",
        )

        records = parse_form4_xml(amended_xml)

        self.assertEqual(len(records), 1)
        self.assertTrue(records[0]["amendment"])

    def test_keeps_duplicate_purchase_transactions(self):
        duplicate_xml = BASE_FORM4.replace(
            "</nonDerivativeTable>",
            """\
    <nonDerivativeTransaction>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>125.5</value></transactionShares>
        <transactionPricePerShare><value>10.20</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>""",
        )

        records = parse_form4_xml(duplicate_xml)

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["purchase_value"], records[1]["purchase_value"])

    def test_returns_unknown_eligibility_without_owner_relationship_metadata(self):
        xml = """\
<ownershipDocument>
  <documentType>4</documentType>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Unknown Owner</rptOwnerName></reportingOwnerId>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>2</value></transactionShares>
        <transactionPricePerShare><value>3</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""

        records = parse_form4_xml(xml)

        self.assertEqual(len(records), 1)
        self.assertIsNone(records[0]["eligible_insider"])
        self.assertEqual(records[0]["purchase_value"], Decimal("6"))

    def test_passive_ten_percent_owner_is_not_eligible(self):
        xml = """\
<ownershipDocument>
  <documentType>4</documentType>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0000000001</rptOwnerCik>
      <rptOwnerName>Passive Owner</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>0</isDirector>
      <isOfficer>0</isOfficer>
      <isTenPercentOwner>1</isTenPercentOwner>
      <isOther>0</isOther>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>2</value></transactionShares>
        <transactionPricePerShare><value>3</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""

        records = parse_form4_xml(xml)

        self.assertEqual(len(records), 1)
        self.assertFalse(records[0]["eligible_insider"])


if __name__ == "__main__":
    unittest.main()
