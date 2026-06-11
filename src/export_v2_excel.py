import logging
from pathlib import Path

from infra.adapters.excel_export_adapter import ExcelExportAdapter
from infra.adapters.parquet_repository_adapter import ParquetRepositoryAdapter
from core.services.data_processing_service import DataProcessingService
from core.services.financial_data_export_service import FinancialDataExportService

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def clear_output_directory(output_dir: Path) -> None:
    """output 디렉토리 안의 모든 파일과 서브디렉토리를 삭제합니다."""
    import shutil
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
        return

    logger.info(f"'{output_dir}' 디렉토리 초기화 중 (모든 파일 삭제)...")
    for item in output_dir.iterdir():
        try:
            if item.is_file() or item.is_symlink():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        except Exception as e:
            logger.error(f"파일 삭제 실패 ({item}): {e}")


def export_v2_integrated():
    """연결 우선 / 개별 보완 방식으로 최종 통합 엑셀 파일을 생성합니다."""
    
    output_dir = Path("output")
    # 1. output 폴더 청소
    clear_output_directory(output_dir)
    
    output_path = output_dir / "financial_data_integrated.xlsx"
    
    # 2. 어댑터 및 서비스 초기화 (이식된 서비스 호출)
    repository_adapter = ParquetRepositoryAdapter()
    export_adapter = ExcelExportAdapter()
    processing_service = DataProcessingService()
    
    export_service = FinancialDataExportService(
        repository_port=repository_adapter,
        export_port=export_adapter,
        processing_service=processing_service
    )
    
    # 3. 통합 내보내기 실행
    success = export_service.export_integrated_financial_data(str(output_path))
    if success:
        logger.info(f"[SUCCESS] 엑셀 통합본 내보내기 완료: {output_path}")
    else:
        logger.error("❌ 엑셀 통합본 내보내기 실패")
        
    # 생성 후 검증
    try:
        remaining_files = list(output_dir.iterdir())
        logger.info(f"output 디렉토리의 최종 파일 목록: {[f.name for f in remaining_files]}")
    except Exception as e:
        logger.error(f"최종 목록 조회 실패: {e}")


if __name__ == "__main__":
    export_v2_integrated()
