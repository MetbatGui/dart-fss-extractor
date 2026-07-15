import io
import re
import zipfile
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional

from core.ports.xbrl_parser_port import XbrlParserPort
from core.domain.models.financial_statement import (
    FinancialStatement,
    FinancialStatementType,
    ReportType,
    AccountItem
)
from core.domain.models.report_period import XbrlPeriodParser

logger = logging.getLogger(__name__)

# 태그 비교 소문자 세트 (금융/제조업 전용 보완)
REVENUE_TAGS = {
    "revenue", "revenuefromcontractswithcustomers", "operatingrevenue", "salesrevenuenet",
    "operatingrevenueoffinancialservices", "revenuefinancialservices", "revenuefrominterest",
    "operatingincomeinsurance", "insurancerevenue"
}
OP_TAGS = {
    "operatingincomeloss", "operatingprofitloss", "operatingprofit", "operatingincome",
    "profitlossfromoperatingactivities"
}
NI_TAGS = {
    "profitloss", "netincome", "profitlossfromcontinuingoperations"
}


class LocalXbrlParserAdapter(XbrlParserPort):
    """로컬 XBRL ZIP 바이너리 파서 어댑터.
    
    - 인메모리 압축 해제를 처리합니다.
    - 한글 라벨 XML을 파싱해 다중 표준명 맵을 구축합니다.
    - Context 구조 분석 및 실질 종료일(Max End Date)을 산출해 CFS/OFS 분기 도메인 객체를 생성합니다.
    """

    def parse_xbrl_zip(
        self,
        zip_data: bytes,
        corp_code: str,
        corp_name: str,
        year: int,
        report_type: ReportType,
        acc_month: int = 12
    ) -> Dict[FinancialStatementType, FinancialStatement]:
        """로컬 공시 원본(XBRL ZIP) 바이너리를 읽어 연결(CFS) 및 개별(OFS) FinancialStatement 객체 사전을 생성합니다."""
        results = {}
        
        # 1. 인메모리 압축 해제
        try:
            zip_buffer = io.BytesIO(zip_data)
            with zipfile.ZipFile(zip_buffer, "r") as z:
                filenames = z.namelist()
                
                # XBRL 파일 및 라벨 파일 식별
                xbrl_file = next((f for f in filenames if f.endswith(".xbrl")), None)
                lab_ko_file = next((f for f in filenames if f.endswith("_lab-ko.xml")), None)
                
                if not xbrl_file:
                    logger.error(f"[{corp_name}] ZIP 파일 내에 .xbrl 파일이 존재하지 않습니다.")
                    return results
                    
                xbrl_data = z.read(xbrl_file)
                lab_ko_data = z.read(lab_ko_file) if lab_ko_file else None
        except Exception as e:
            logger.error(f"[{corp_name}] XBRL ZIP 압축 해제 실패: {e}")
            return results

        # 2. 한글 라벨 사전 구축
        label_map = self._build_label_map(lab_ko_data) if lab_ko_data else {}

        # 3. XBRL ElementTree 파싱 및 네임스페이스 정의
        try:
            root = ET.fromstring(xbrl_data)
        except Exception as e:
            logger.error(f"[{corp_name}] XBRL XML 파싱 실패: {e}")
            return results

        namespaces = {
            'xbrli': 'http://www.xbrl.org/2003/instance',
            'xbrldi': 'http://xbrl.org/2006/xbrldi'
        }

        # 4. 컨텍스트 및 분기 마감일(Max End Date) 추출
        contexts = self._get_contexts(root, namespaces)
        
        for want_consolidated in [True, False]:
            fs_type = FinancialStatementType.CONSOLIDATED if want_consolidated else FinancialStatementType.SEPARATE
            
            # 대상 컨텍스트의 마감일 확인
            valid_dates = []
            for ctx in contexts.values():
                if ctx["type"] == "duration" and ctx.get("is_pure", True) and ctx["is_consolidated"] == want_consolidated:
                    try:
                        e_d = datetime.strptime(ctx["end_date"], "%Y-%m-%d").date()
                        valid_dates.append(e_d)
                    except Exception:
                        pass
            
            if not valid_dates:
                continue
                
            max_end_date = max(valid_dates)
            
            # 실질 타겟 누적 개월수 계산 (모듈러 공식)
            target_cum_months = (max_end_date.month - acc_month) % 12
            if target_cum_months == 0:
                target_cum_months = 12

            # 5. 수치 정보 수집
            rev_cur, rev_cum = None, None
            op_cur, op_cum = None, None
            ni_cur, ni_cum = None, None
            
            for elem in root.iter():
                tag_local = elem.tag.split('}')[-1]
                tag_lower = tag_local.lower()
                
                is_rev = tag_lower in REVENUE_TAGS
                is_op = tag_lower in OP_TAGS
                is_ni = tag_lower in NI_TAGS
                
                # 라벨 대조 매핑
                ko_label = label_map.get(tag_lower, "")
                if ko_label:
                    ko_label_clean = ko_label.replace(" ", "").replace(".", "")
                    if not is_rev and ko_label_clean in ["매출액", "영업수익", "매출액(매출총이익)"]:
                        is_rev = True
                    if not is_op and ko_label_clean in ["영업이익", "영업손실", "영업이익(손실)", "영업손익"]:
                        is_op = True
                    if not is_ni and ko_label_clean in [
                        "당기순이익", "당기순손실", "당기순이익(손실)", "분기순이익", "반기순이익", "당기순손익",
                        "지배기업소유주지분순이익", "지배기업주주지분순이익", "지배주주순이익", "지배주주순이익(손실)"
                    ]:
                        is_ni = True
                
                if not (is_rev or is_op or is_ni):
                    continue
                    
                val_str = (elem.text or "").strip()
                if not val_str or val_str == "-":
                    continue
                    
                ctx_ref = elem.get("contextRef")
                if not ctx_ref or ctx_ref not in contexts:
                    continue
                    
                ctx = contexts[ctx_ref]
                if ctx["type"] != "duration":
                    continue
                    
                if not ctx.get("is_pure", True) or ctx["is_consolidated"] != want_consolidated:
                    continue
                    
                try:
                    s_date = datetime.strptime(ctx["start_date"], "%Y-%m-%d").date()
                    e_date = datetime.strptime(ctx["end_date"], "%Y-%m-%d").date()
                except Exception:
                    continue
                    
                if e_date != max_end_date:
                    continue
                    
                ctx_months = (e_date.year - s_date.year) * 12 + (e_date.month - s_date.month) + 1
                
                try:
                    val_int = int(float(val_str))
                except Exception:
                    continue
                
                # 수치 저장 (당기 단기 3M vs YTD 누적)
                if is_rev:
                    if ctx_months == 3:
                        rev_cur = val_int
                    if ctx_months == target_cum_months:
                        rev_cum = val_int
                elif is_op:
                    if ctx_months == 3:
                        op_cur = val_int
                    if ctx_months == target_cum_months:
                        op_cum = val_int
                elif is_ni:
                    if ctx_months == 3:
                        ni_cur = val_int
                    if ctx_months == target_cum_months:
                        ni_cum = val_int

            # 6. 도메인 구조화 및 폴백 보완
            # 누적치는 존재하나 3M 단기 수치가 비어 있는 경우, YTD 누적치로부터 폴백 복원 (1Q 등의 동일성 가드)
            if rev_cur is None and rev_cum is not None:
                rev_cur = rev_cum
            if op_cur is None and op_cum is not None:
                op_cur = op_cum
            if ni_cur is None and ni_cum is not None:
                ni_cur = ni_cum

            accounts = []
            if rev_cur is not None or rev_cum is not None:
                accounts.append(AccountItem("매출액", str(rev_cur or 0), str(rev_cum or 0), statement_type="IS"))
            if op_cur is not None or op_cum is not None:
                accounts.append(AccountItem("영업이익", str(op_cur or 0), str(op_cum or 0), statement_type="IS"))
            if ni_cur is not None or ni_cum is not None:
                accounts.append(AccountItem("당기순이익", str(ni_cur or 0), str(ni_cum or 0), statement_type="IS"))

            # 기간 시작일 복원용
            start_date_c = None
            for ctx in contexts.values():
                if ctx["type"] == "duration" and ctx.get("is_pure", True) and ctx["is_consolidated"] == want_consolidated:
                    try:
                        e_d = datetime.strptime(ctx["end_date"], "%Y-%m-%d").date()
                        s_d = datetime.strptime(ctx["start_date"], "%Y-%m-%d").date()
                        if e_d == max_end_date:
                            ctx_months = (e_d.year - s_d.year) * 12 + (e_d.month - s_d.month) + 1
                            if ctx_months == target_cum_months:
                                start_date_c = s_d
                                break
                    except Exception:
                        pass

            if accounts:
                stmt = FinancialStatement(
                    corp_code=corp_code,
                    corp_name=corp_name,
                    bsns_year=year,
                    reprt_type=report_type,
                    fs_type=fs_type,
                    accounts=accounts,
                    start_date=start_date_c,
                    end_date=max_end_date,
                    is_cumulative=True
                )
                results[fs_type] = stmt

        return results

    def _build_label_map(self, lab_data: bytes) -> Dict[str, str]:
        """한글 라벨 XML 데이터를 파싱하여 concept -> 한글 텍스트 사전을 구성합니다."""
        label_map = {}
        try:
            ns = {
                'link': 'http://www.xbrl.org/2003/linkbase',
                'xlink': 'http://www.w3.org/1999/xlink'
            }
            root = ET.fromstring(lab_data)
            
            loc_to_concept = {}
            for loc in root.findall('.//link:loc', ns):
                href = loc.get('{http://www.w3.org/1999/xlink}href') or ""
                loc_id = loc.get('{http://www.w3.org/1999/xlink}label')
                concept_name = href.split('#')[-1] if '#' in href else href
                if loc_id and concept_name:
                    concept_clean = re.sub(r'^(entity\d+|dge\d+|dar\d+)_', '', concept_name)
                    loc_to_concept[loc_id] = concept_clean
                    
            loc_to_label_id = {}
            for arc in root.findall('.//link:labelArc', ns):
                from_id = arc.get('{http://www.w3.org/1999/xlink}from')
                to_id = arc.get('{http://www.w3.org/1999/xlink}to')
                if from_id and to_id:
                    loc_to_label_id[from_id] = to_id
                    
            for lbl in root.findall('.//link:label', ns):
                lbl_id = lbl.get('id') or lbl.get('{http://www.w3.org/1999/xlink}label')
                lbl_text = (lbl.text or "").strip()
                for loc_id, to_id in loc_to_label_id.items():
                    if to_id == lbl_id:
                        concept = loc_to_concept.get(loc_id)
                        if concept and lbl_text:
                            label_map[concept.lower()] = lbl_text
        except Exception as e:
            logger.warning(f"라벨 맵 생성 중 경고: {e}")
        return label_map

    def _get_contexts(self, root: ET.Element, namespaces: dict) -> Dict[str, Dict]:
        """XBRL 파일 내의 컨텍스트 정보를 읽어와 연결/개별 구분 및 순수 손익계산서 여부를 판별합니다."""
        contexts = {}
        for ctx in root.findall('.//xbrli:context', namespaces):
            ctx_id = ctx.get('id')
            if not ctx_id:
                continue
            period = ctx.find('xbrli:period', namespaces)
            if period is None:
                continue
            
            is_consolidated = True
            is_pure = True
            
            entity = ctx.find('xbrli:entity', namespaces)
            if entity is not None:
                segment = entity.find('xbrli:segment', namespaces)
                if segment is not None:
                    members = segment.findall('.//xbrldi:explicitMember', namespaces)
                    has_separate = False
                    has_consolidated = False
                    has_other_axis = False
                    
                    for mem in members:
                        dim = mem.get("dimension") or ""
                        val = (mem.text or "").strip()
                        
                        if "ConsolidatedAndSeparateFinancialStatementsAxis" in dim:
                            if "SeparateMember" in val:
                                has_separate = True
                            elif "ConsolidatedMember" in val:
                                has_consolidated = True
                        else:
                            has_other_axis = True
                    
                    if has_separate:
                        is_consolidated = False
                    elif has_consolidated:
                        is_consolidated = True
                    else:
                        is_consolidated = True
                        
                    if has_other_axis:
                        is_pure = False
                            
            instant = period.find('xbrli:instant', namespaces)
            if instant is not None:
                contexts[ctx_id] = {"type": "instant", "is_consolidated": is_consolidated, "is_pure": is_pure}
            else:
                s_date = period.find('xbrli:startDate', namespaces)
                e_date = period.find('xbrli:endDate', namespaces)
                if s_date is not None and e_date is not None:
                    contexts[ctx_id] = {
                        "type": "duration",
                        "start_date": s_date.text.strip(),
                        "end_date": e_date.text.strip(),
                        "is_consolidated": is_consolidated,
                        "is_pure": is_pure
                    }
        return contexts
