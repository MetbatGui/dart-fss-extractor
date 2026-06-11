"""파일 데이터를 원격/로컬 스토리지에 저장하고 읽기 위한 포트 인터페이스."""

from abc import ABC, abstractmethod
from typing import Optional, List
import pandas as pd
import openpyxl


class StoragePort(ABC):
    """파일 저장 및 다운로드를 처리하는 스토리지 포트 인터페이스."""

    @abstractmethod
    def save_dataframe_excel(self, df: pd.DataFrame, path: str, **kwargs) -> bool:
        """DataFrame을 Excel 파일로 저장합니다.

        Args:
            df: 저장할 DataFrame.
            path: 저장할 파일 경로.
            **kwargs: 추가 옵션.

        Returns:
            성공 여부.
        """
        raise NotImplementedError

    @abstractmethod
    def save_dataframe_csv(self, df: pd.DataFrame, path: str, **kwargs) -> bool:
        """DataFrame을 CSV 파일로 저장합니다.

        Args:
            df: 저장할 DataFrame.
            path: 저장할 파일 경로.
            **kwargs: 추가 옵션.

        Returns:
            성공 여부.
        """
        raise NotImplementedError

    @abstractmethod
    def save_workbook(self, book: openpyxl.Workbook, path: str) -> bool:
        """openpyxl Workbook을 저장합니다.

        Args:
            book: 저장할 Workbook.
            path: 저장할 파일 경로.

        Returns:
            성공 여부.
        """
        raise NotImplementedError

    @abstractmethod
    def load_workbook(self, path: str) -> Optional[openpyxl.Workbook]:
        """Excel Workbook을 로드합니다.

        Args:
            path: 로드할 파일 경로.

        Returns:
            성공 시 openpyxl.Workbook, 실패 시 None.
        """
        raise NotImplementedError

    @abstractmethod
    def path_exists(self, path: str) -> bool:
        """경로가 존재하는지 확인합니다.

        Args:
            path: 확인할 경로.

        Returns:
            존재 여부.
        """
        raise NotImplementedError

    @abstractmethod
    def ensure_directory(self, path: str) -> bool:
        """디렉토리가 존재하는지 보장하고 없으면 생성합니다.

        Args:
            path: 디렉토리 경로.

        Returns:
            성공 여부.
        """
        raise NotImplementedError

    @abstractmethod
    def load_dataframe(self, path: str, sheet_name: str = None, **kwargs) -> pd.DataFrame:
        """Excel 파일에서 DataFrame을 로드합니다.

        Args:
            path: 파일 경로.
            sheet_name: 시트 이름.
            **kwargs: 추가 옵션.

        Returns:
            로드된 DataFrame.
        """
        raise NotImplementedError

    @abstractmethod
    def get_file(self, path: str) -> Optional[bytes]:
        """파일의 내용을 바이트 데이터로 읽어옵니다.

        Args:
            path: 파일 경로.

        Returns:
            파일 바이너리 데이터, 실패 시 None.
        """
        raise NotImplementedError

    @abstractmethod
    def put_file(self, path: str, data: bytes) -> bool:
        """바이트 데이터를 파일로 업로드합니다.

        Args:
            path: 저장할 파일 경로.
            data: 저장할 바이트 데이터.

        Returns:
            성공 여부.
        """
        raise NotImplementedError

    @abstractmethod
    def list_files(self, directory_path: str) -> List[str]:
        """디렉토리 내의 파일 목록을 반환합니다.

        Args:
            directory_path: 디렉토리 경로.

        Returns:
            파일명 리스트.
        """
        raise NotImplementedError
