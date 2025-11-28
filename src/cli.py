"""CLI 인터페이스."""

import os
import sys
import logging
from pathlib import Path
from typing import Optional, List

import typer
import pandas as pd
from rich.console import Console
from dotenv import load_dotenv

# src 디렉토리를 모듈 검색 경로에 추가
sys.path.append(str(Path(__file__).parent))

from core.services.financial_collection_service import FinancialCollectionService
from core.services.data_processing_service import DataProcessingService
from infra.adapters.dart_financial_adapter import DartFinancialAdapter
from infra.adapters.corp_code_adapter import CorpCodeAdapter
from infra.adapters.local_storage_adapter import LocalStorageAdapter

# Typer 앱 생성
app = typer.Typer(
    name="dart-fss-extractor",
    help="DART FSS 재무제표 데이터 수집 도구",
    add_completion=False
)

# Rich console
console = Console()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# 기본 설정
DEFAULT_START_YEAR = 2015
DEFAULT_END_YEAR = 2025  # 2025년까지 수집 (2015~2025, 11년치)
DEFAULT_TARGET_FILE = Path("data/target_companies.csv")


@app.command()
def collect(
    start_year: int = typer.Option(DEFAULT_START_YEAR, "--start-year", "-s", help="수집 시작 연도"),
    end_year: int = typer.Option(DEFAULT_END_YEAR, "--end-year", "-e", help="수집 종료 연도 (포함)"),
    companies: Optional[str] = typer.Option(None, "--companies", "-c", help="수집할 기업명 (쉼표로 구분)"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="출력 파일 경로"),
):
    """재무 데이터를 수집합니다.
    
    기본값: 2015~2025년 (11년치) 데이터를 수집합니다.
    
    Examples:
        $ uv run collector collect
        $ uv run collector collect --start-year 2020 --end-year 2024
        $ uv run collector collect --companies "삼성전자,SK하이닉스"
    """
    # .env 파일 로드
    load_dotenv()
    
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        console.print("[red]❌ DART_API_KEY 환경 변수가 설정되지 않았습니다.[/red]")
        logger.error("DART_API_KEY 환경 변수가 설정되지 않았습니다.")
        raise typer.Exit(code=1)
    
    # 기업 목록 로드
    if companies:
        # 명령줄에서 직접 지정
        company_names = [c.strip() for c in companies.split(",")]
        console.print(f"[cyan]📋 수집 대상: {', '.join(company_names)}[/cyan]")
    else:
        # CSV 파일에서 로드
        company_names = _load_companies_from_file()
        console.print(f"[cyan]📋 {DEFAULT_TARGET_FILE}에서 {len(company_names)}개 기업 로드[/cyan]")
    
    # 출력 경로 설정
    if not output:
        output = f"output/financial_data_{start_year}_{end_year}.xlsx"
    
    # 실제 수집 기간 표시
    console.print(f"[yellow]📅 수집 기간: {start_year}년 ~ {end_year}년 ({end_year-start_year+1}년치)[/yellow]")
    
    # 서비스 초기화
    console.print("[yellow]⚙️  서비스 초기화 중...[/yellow]")
    logger.info("서비스 초기화 중...")
    
    try:
        # 어댑터 초기화
        corp_code_adapter = CorpCodeAdapter()
        financial_adapter = DartFinancialAdapter(api_key=api_key, use_cache=True)
        storage_adapter = LocalStorageAdapter()
        processing_service = DataProcessingService()
        
        # 서비스 초기화
        service = FinancialCollectionService(
            corp_code_port=corp_code_adapter,
            financial_port=financial_adapter,
            storage_port=storage_adapter,
            processing_service=processing_service
        )
        
        # 데이터 수집
        console.print(f"[green]🚀 데이터 수집 시작: {len(company_names)}개 기업[/green]")
        logger.info(f"데이터 수집 시작: {len(company_names)}개 기업, {start_year}~{end_year}년")
        
        service.collect_and_save(
            company_names=company_names,
            start_year=start_year,
            end_year=end_year,
            output_path=output
        )
        
        console.print(f"[green]✅ 완료! 결과 저장: {output}[/green]")
        logger.info("모든 작업이 완료되었습니다.")
    
    except Exception as e:
        console.print(f"[red]❌ 오류 발생: {e}[/red]")
        logger.exception(f"작업 중 치명적인 오류 발생: {e}")
        raise typer.Exit(code=1)


def _load_companies_from_file() -> List[str]:
    """CSV 파일에서 기업 목록을 로드합니다."""
    if DEFAULT_TARGET_FILE.exists():
        try:
            df = pd.read_csv(DEFAULT_TARGET_FILE)
            if "기업명" in df.columns:
                company_names = df["기업명"].tolist()
                return company_names
            else:
                logger.warning("target_companies.csv에 '기업명' 컬럼이 없습니다. 기본 리스트를 사용합니다.")
                return _get_default_companies()
        except Exception as e:
            logger.error(f"기업 목록 파일 읽기 실패: {e}")
            return _get_default_companies()
    else:
        logger.info("target_companies.csv 파일이 없습니다. 기본 리스트를 사용합니다.")
        # 기본 리스트 생성 및 저장
        company_names = _get_default_companies()
        DEFAULT_TARGET_FILE.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"기업명": company_names}).to_csv(DEFAULT_TARGET_FILE, index=False)
        logger.info(f"기본 기업 목록을 {DEFAULT_TARGET_FILE}에 저장했습니다.")
        return company_names


def _get_default_companies() -> List[str]:
    """기본 기업 목록을 반환합니다."""
    return ["삼성전자", "SK하이닉스", "LG에너지솔루션", "현대차", "NAVER"]


if __name__ == "__main__":
    app()
