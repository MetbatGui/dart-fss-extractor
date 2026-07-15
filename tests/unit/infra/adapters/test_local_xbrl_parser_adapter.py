import io
import os
import zipfile
import pytest
from datetime import date
from core.domain.models.financial_statement import ReportType, FinancialStatementType, FinancialStatement

# TDD 1단계: 실패를 위한 신규 어댑터 임포트 시도
from infra.adapters.local_xbrl_parser_adapter import LocalXbrlParserAdapter


def create_dummy_xbrl_zip() -> bytes:
    """테스트용 가상 XBRL ZIP 바이너리를 인메모리로 생성합니다."""
    # 1. 한글 라벨 XML 본문
    dummy_lab_ko = (
        b'<?xml version="1.0" encoding="UTF-8"?>\n'
        b'<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" '
        b'xmlns:xlink="http://www.w3.org/1999/xlink">\n'
        b'  <link:loc xlink:type="locator" xlink:href="http://example.com#ifrs-full_Revenue" xlink:label="loc_Revenue"/>\n'
        b'  <link:loc xlink:type="locator" xlink:href="http://example.com#ifrs-full_OperatingProfit" xlink:label="loc_OperatingProfit"/>\n'
        b'  <link:labelArc xlink:type="arc" xlink:arcrole="http://www.xbrl.org/2003/arcrole/concept-label" '
        b'xlink:from="loc_Revenue" xlink:to="lbl_Revenue"/>\n'
        b'  <link:labelArc xlink:type="arc" xlink:arcrole="http://www.xbrl.org/2003/arcrole/concept-label" '
        b'xlink:from="loc_OperatingProfit" xlink:to="lbl_OperatingProfit"/>\n'
        b'  <link:label xlink:type="resource" xlink:label="lbl_Revenue" xml:lang="ko">\xeb\xa7\xa4\xec\xb3\x9c\xec\x95\xa1</link:label>\n' # 매출액
        b'  <link:label xlink:type="resource" xlink:label="lbl_OperatingProfit" xml:lang="ko">\xec\x98\x81\xec\x97\x85\xec\x9d\xb4\xec\x9d\xb5</link:label>\n' # 영업이익
        b'</link:linkbase>'
    )

    # 2. XBRL 본문 XML
    # CFY2025dFQA -> 당기 1분기 YTD 누적(3개월) 컨텍스트 패턴 모사
    dummy_xbrl = (
        b'<?xml version="1.0" encoding="UTF-8"?>\n'
        b'<xbrl xmlns="http://www.xbrl.org/2003/instance" '
        b'xmlns:xbrli="http://www.xbrl.org/2003/instance" '
        b'xmlns:xbrldi="http://xbrl.org/2006/xbrldi" '
        b'xmlns:ifrs-full="http://xbrl.ifrs.org/taxonomy/2015-03-11/ifrs-full">\n'
        b'  <xbrli:context id="CFY2025dFQA">\n'
        b'    <xbrli:entity>\n'
        b'      <xbrli:identifier scheme="http://dart.fss.or.kr/corp_code">00126380</xbrli:identifier>\n'
        b'      <xbrli:segment>\n'
        b'        <xbrldi:explicitMember dimension="ifrs-full:ConsolidatedAndSeparateFinancialStatementsAxis">ifrs-full:ConsolidatedMember</xbrldi:explicitMember>\n'
        b'      </xbrli:segment>\n'
        b'    </xbrli:entity>\n'
        b'    <xbrli:period>\n'
        b'      <xbrli:startDate>2025-01-01</xbrli:startDate>\n'
        b'      <xbrli:endDate>2025-03-31</xbrli:endDate>\n'
        b'    </xbrli:period>\n'
        b'  </xbrli:context>\n'
        # 매출액 태그 데이터 적재 (1,500억)
        b'  <ifrs-full:Revenue contextRef="CFY2025dFQA" unitRef="KRW" decimals="-6">150000000000</ifrs-full:Revenue>\n'
        # 영업이익 태그 데이터 적재 (120억)
        b'  <ifrs-full:OperatingProfit contextRef="CFY2025dFQA" unitRef="KRW" decimals="-6">12000000000</ifrs-full:OperatingProfit>\n'
        b'</xbrl>'
    )

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        zf.writestr("test_report_lab-ko.xml", dummy_lab_ko)
        zf.writestr("test_report.xbrl", dummy_xbrl)
        
    return zip_buffer.getvalue()


def test_parse_xbrl_zip_success():
    """올바른 테스트 ZIP 데이터가 주어졌을 때 FinancialStatement 도메인 객체가 복원되는지 검증."""
    zip_bytes = create_dummy_xbrl_zip()
    adapter = LocalXbrlParserAdapter()
    
    # 2025년 1분기보고서(11013), 결산월 12월
    result = adapter.parse_xbrl_zip(
        zip_data=zip_bytes,
        corp_code="00126380",
        corp_name="삼성전자",
        year=2025,
        report_type=ReportType.Q1,
        acc_month=12
    )
    
    assert FinancialStatementType.CONSOLIDATED in result
    cfs_stmt = result[FinancialStatementType.CONSOLIDATED]
    
    assert cfs_stmt.corp_code == "00126380"
    assert cfs_stmt.corp_name == "삼성전자"
    assert cfs_stmt.bsns_year == 2025
    assert cfs_stmt.reprt_type == ReportType.Q1
    
    # 계정과목 확인
    rev_amount = cfs_stmt.find_account_amount(["Revenue", "매출액"])
    op_amount = cfs_stmt.find_account_amount(["OperatingProfit", "영업이익"])
    
    assert not rev_amount.is_none
    assert int(rev_amount) == 150000000000
    assert not op_amount.is_none
    assert int(op_amount) == 12000000000
    
    # 날짜 범위 복원 확인
    assert cfs_stmt.start_date == date(2025, 1, 1)
    assert cfs_stmt.end_date == date(2025, 3, 31)
