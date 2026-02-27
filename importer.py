from csv import DictReader
from dataclasses import dataclass
from os import scandir, sep, DirEntry
from os.path import dirname, exists, join as path_join
from sys import exit, stderr
from typing import Dict, List, Tuple, OrderedDict
from unittest import case

from dotenv import dotenv_values


@dataclass
class Config:
    def __init__(self, config: Dict):
        self.cash_book_path = ''
        self.notimon_log_path = ''

        if 'CASH_BOOK_PATH' in config:
            self.cash_book_path = config['CASH_BOOK_PATH'].rstrip(sep)
        if not self.cash_book_path or not exists(self.cash_book_path):
            raise FileNotFoundError('CASH_BOOK_PATH is invalid')

        if 'NOTIMON_LOG_PATH' in config:
            self.notimon_log_path = config['NOTIMON_LOG_PATH'].rstrip(sep)
        if not self.notimon_log_path or not exists(self.notimon_log_path):
            raise FileNotFoundError('NOTIMON_LOG_PATH is invalid')


@dataclass
class LogRecord:
    def __init__(self, **kwargs):
        self.timestamp: str = kwargs.get('Timestamp', '')
        self.package: str = kwargs.get('Package', '')
        self.title: str = kwargs.get('Title', '')
        self.text: str = kwargs.get('Text', '')


class NotimonLogScan:
    def __init__(self, notimon_log_path: str, marker: Tuple[int, int, int]):
        self.notimon_log_path = notimon_log_path
        self.path_len = len(self.notimon_log_path)

        self.last_year = marker[0]
        self.last_month = marker[1]
        self.last_day = marker[2]

        self.files = []

    def scan(self) -> List[str]:
        self.files = []
        self._deep_scan(self.notimon_log_path)

        return self.files

    def _deep_scan(self, path: str):
        with scandir(path) as iterator:
            for it in iterator:
                if it.is_dir() and self._is_targeted_dir(it):
                    self._deep_scan(it.path)
                elif it.is_file() and self._is_targeted_file(it):
                    self.files.append(it.path)

    def _is_targeted_dir(self, entry: DirEntry) -> bool:
        rel_path = entry.path[len(self.notimon_log_path):].strip(sep)
        dirs = rel_path.split(sep)

        match len(dirs):
            case 1:
                return self._is_valid_value(dirs[0], self.last_year)
            case 2:
                return self._is_valid_value(dirs[0], self.last_year) and \
                    self._is_valid_value(dirs[1], self.last_month)
            case _:
                return False

    def _is_targeted_file(self, entry: DirEntry) -> bool:
        parts = entry.name.split('.')[0].split('-')

        return 3 == len(parts) and \
            self._is_valid_value(parts[0], self.last_year) and \
            self._is_valid_value(parts[1], self.last_month) and \
            self._is_valid_value(parts[2], self.last_day)

    @staticmethod
    def _is_valid_value(file_name_val: str, marker_val: int) -> bool:
        return file_name_val.isdigit() and int(file_name_val) >= marker_val


class NotimonLogRead:
    def __init__(self, notimon_log_path: str, marker: Tuple[int, int, int]):
        self.notimon_log_path = notimon_log_path
        self.last_day = marker[2]
        self.found: OrderedDict[str, List[Dict[str, str]]] = OrderedDict()

    def read(self, file_paths: List[str]) -> OrderedDict[str, List[Dict[str, str]]]:
        for file_path in file_paths:
            # CSV file yyyy-mm-dd.csv
            name = file_path.split(sep)[-1].split('.')[0]
            name_parts = name.split('-')
            if 3 == len(name_parts) and name_parts[2].isdigit and int(name_parts[2]) >= self.last_day:
                self.found[name] = self._single_read(file_path)

        return self.found

    @staticmethod
    def _single_read(file_path: str) -> List[Dict[str, str]]:
        if not exists(file_path):
            raise Exception(f'File not found: {file_path}')

        with open(file_path, mode='r', newline='', encoding='UTF8') as f:
            reader = DictReader(f, ['Timestamp', 'Package', 'Title', 'Text'])
            next(reader)  # Discard the header
            rows = [row for row in reader]

        return rows


class CashBookMarker:
    def __init__(self, cash_book_path: str):
        self.cash_book_path = cash_book_path

    def get_info(self) -> Tuple[int, int, int]:
        marker_path = path_join(self.cash_book_path, 'marker.md')

        if not exists(marker_path):
            return 0, 0, 0

        with open(marker_path, 'r') as f:
            items = [x.strip() for x in f.read().split('-')]

        if 3 != len(items) or not items[0].isdigit() or not items[1].isdigit():
            return 0, 0, 0

        return int(items[0]), int(items[1]), int(items[2])


class CashBookImporter:
    def __init__(self, cash_book_path: str):
        self.cash_book_path = cash_book_path

    def import_to_cashbook(self, records: List[Dict[str, str]]):
        for record in records:
            r = LogRecord(**record)

            if self.has_record(r):
                continue

            match r.package:
                case 'com.wooribank.smart.npib':
                    data = self.parse_wooribank(r)
                    pass


    def has_record(self, r):
        pass

    def pars_wooribank(self):
        pass


def import_to_cashbook():
    config = Config(dotenv_values())

    # Get marker info
    marker = CashBookMarker(config.cash_book_path).get_info()

    # Get target files
    files = NotimonLogScan(config.notimon_log_path, marker).scan()

    # Read CSV records
    daily_records = NotimonLogRead(
        notimon_log_path=config.notimon_log_path,
        marker=marker
    ).read(files)

    importer = CashBookImporter(config.notimon_log_path)

    for date, records in daily_records.items():
        importer.import_to_cashbook(records)


if '__main__' == __name__:
    try:
        import_to_cashbook()
    except Exception as e:
        print(e, file=stderr)
        exit(1)
