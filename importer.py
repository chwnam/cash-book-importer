from csv import DictReader
from dataclasses import dataclass
from os import makedirs, scandir, sep, DirEntry
from os.path import dirname, exists, join as path_join, relpath
from re import match, compile as re_compile, escape as re_escape
from sys import exit, stderr
from typing import Dict, List, OrderedDict, Union

from dotenv import dotenv_values

version = '1.0.0'

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


@dataclass
class CashBookEntry:
    path: str
    timestamp: str
    package: str
    title: str  # LogRecord.title 과 다름
    category: str
    description: str
    amount: int
    account: str
    balance: int

    def __init__(self):
        self.path = ''
        self.timestamp = ''
        self.package = ''
        self.title = ''
        self.category = ''
        self.description = ''
        self.amount = 0
        self.account = ''
        self.balance = 0


class Marker:
    expr = re_compile(r'^(\d{4})-(\d{2})-(\d{2})$')

    def __init__(self, cash_book_path: str):
        self._year: int = 0
        self._month: int = 0
        self._day: int = 0

        marker_path = path_join(cash_book_path, 'marker.md')
        if not exists(marker_path):
            return

        with open(marker_path, 'r') as f:
            m = self.expr.match(f.read().strip())
            if m:
                self._year = int(m.group(1))
                self._month = int(m.group(2))
                self._day = int(m.group(3))

    @property
    def year(self):
        return self._year

    @property
    def month(self):
        return self._month

    @property
    def day(self):
        return self._day


class NotimonLogScan:
    def __init__(self, notimon_log_path: str, marker: Marker):
        s = re_escape(sep)
        self._dir_expr = re_compile(r'^\d{4}(?:' + s + r'\d{2})?$')
        self._file_expr = re_compile(r'^\d{4}' + s + r'\d{2}' + s + r'\d{4}-\d{2}-\d{2}\.csv$')

        self._notimon_log_path = notimon_log_path
        self._path_len = len(self._notimon_log_path)
        self._marker = marker

        self._files: List[DirEntry] = []

    def scan(self) -> List[DirEntry]:
        self._files = []
        self._deep_scan(self._notimon_log_path, 1)

        return self._files

    def _deep_scan(self, path: str, depth: int):
        with scandir(path) as iterator:
            for entry in iterator:
                if entry.is_dir() and self._is_year_month_dir(entry, depth):
                    self._deep_scan(entry.path, depth + 1)
                elif entry.is_file() and self._is_targeted_file(entry):
                    self._files.append(entry)

    def _is_year_month_dir(self, entry: DirEntry, depth: int) -> bool:
        if depth > 2:
            return False

        p = relpath(entry.path, self._notimon_log_path)
        if not match(self._dir_expr, p):
            return False

        dirs = list(map(int, p.split(sep)))
        match len(dirs):
            case 1:
                return dirs[0] >= self._marker.year
            case 2:
                return dirs[0] > self._marker.year or \
                    (dirs[0] == self._marker.year and dirs[1] >= self._marker.month)
            case _:
                return False

    def _is_targeted_file(self, entry: DirEntry) -> bool:
        p = relpath(entry.path, self._notimon_log_path)
        if not match(self._file_expr, p):
            return False

        parts = list(map(int, entry.name.split('.')[0].split('-')))

        return parts[0] > self._marker.year or \
            (parts[0] == self._marker.year and parts[1] > self._marker.month) or \
            (parts[0] == self._marker.year and parts[1] == self._marker.month and parts[2] >= self._marker.day)


class NotimonLogRead:
    def __init__(self, notimon_log_path: str, marker: Marker):
        self._notimon_log_path = notimon_log_path
        self._marker = marker
        self._found: OrderedDict[str, List[Dict[str, str]]] = OrderedDict()

    def read(self, entries: List[DirEntry]) -> OrderedDict[str, List[Dict[str, str]]]:
        self._found = OrderedDict()

        for entry in entries:
            if not exists(entry.path):
                raise Exception(f'File not found: {entry.path}')

            # CSV file yyyy-mm-dd.csv
            name = entry.name.split('.')[0]

            with open(entry.path, mode='r', newline='', encoding='UTF8') as f:
                reader = DictReader(f, ['Timestamp', 'Package', 'Title', 'Text'])
                next(reader)  # Discard the header
                self._found[name] = [row for row in reader]

        return self._found


class Parser:
    def __init__(self, cash_book_path: str):
        self.cash_book_path = cash_book_path

    def parse(self, r: LogRecord) -> Union[CashBookEntry, None]:
        raise NotImplementedError()


class WooriParser(Parser):
    """
    우리 WON 뱅킹 입출금 알림 텍스트
    """
    date_expr = re_compile(r'^\d{2}/\d{2}$')
    time_expr = re_compile(r'^\d{2}:\d{2}:\d{2}$')
    value_expr = re_compile(r'^[\d,]+원$')
    account_expr = re_compile(r'^[\d\-]+\*{3}계좌$')

    def parse(self, record: LogRecord) -> Union[CashBookEntry, None]:
        """
        텍스트 예시:
          [출금] 지에스２５　도곡 2,400원 1002-123-456***계좌 잔액 8,525원 02/26 01:13:38

        공백으로 나누면 최소 8개의 길이를 가진 리스트가 됨
        """
        if 'com.wooribank.smart.npib' != record.package or '우리WON뱅킹 입출금알림' != record.title:
            return None

        parts = record.text.split(' ')

        if len(parts) < 8 and not ('[입금]' == parts[0] or '[출금]' == parts[1]):
            return None

        factor: int = 1 if '[입금]' == parts[0] else -1

        parts.reverse()
        # 0: 시간
        # 1: 날짜
        # 2: 잔액
        # 3: '잔액' 고정 텍스트
        # 4: 계좌
        # 5: 금액
        # 6~: 출처
        # -1: [입금], [출금] 고정 텍스트

        # 마지막 테스트 체크
        # 시간 포맷
        if not self.time_expr.match(parts[0]):
            return None
        # 날짜 포맷
        if not self.date_expr.match(parts[1]):
            return None
        # 잔액 포맷
        if not self.value_expr.match(parts[2]):
            return None
        # '잔액' 고정 텍스트
        if '잔액' != parts[3]:
            return None
        # 계좌 포맷
        if not self.account_expr.match(parts[4]):
            return None
        # 금액 포맷
        if not self.value_expr.match(parts[5]):
            return None

        entry = CashBookEntry()

        # yyyy-mm-dd HH:ii:ss ---> yyyy-mm-dd HH-ii-ss
        entry.path = path_join(
            self.cash_book_path,
            record.timestamp[0:7].replace('-', sep),
            record.timestamp.replace(':', '-') + '.md'
        )

        # 문자메시지에서 기록된 날짜를 기준으로
        entry.timestamp = f'{record.timestamp[0:4]}-{parts[1].replace('/', '-')}T{parts[0]}'

        entry.package = record.package
        entry.title = ' '.join(parts[-2:5:-1])
        entry.description = ''
        entry.amount = int(parts[5][0:-1].replace(',', '')) * factor
        entry.account = parts[4][0:-2]  # '계좌' 텍스트 제거
        entry.balance = int(parts[2][0:-1].replace(',', ''))

        return entry


class Importer:
    def __init__(self, cash_book_path: str):
        self.cash_book_path = cash_book_path

    def import_to_cashbook(self, records: List[Dict[str, str]]) -> Union[CashBookEntry, None]:
        for record in records:
            r = LogRecord(**record)

            match r.package:
                case 'com.wooribank.smart.npib':
                    entry = WooriParser(self.cash_book_path).parse(r)
                case _:
                    entry = None

            if entry and not exists(entry.path):
                self._create_markdown(entry)

    @staticmethod
    def _create_markdown(entry: CashBookEntry):
        if not exists(dirname(entry.path)):
            makedirs(dirname(entry.path), mode=0o755)

        with open(entry.path, mode='w', encoding='utf-8') as f:
            f.write('---\n')  # start the properties
            f.write(f'일시: {entry.timestamp}\n')
            f.write(f'패키지: {entry.package}\n')
            f.write(f'항목: {entry.title}\n')
            f.write(f'입출금분류: {entry.category}\n')
            f.write(f'설명: {entry.description}\n')
            f.write(f'금액: {entry.amount}\n')
            f.write(f'계좌: {entry.account}\n')
            f.write(f'잔액(알림): {entry.balance}\n')
            f.write('---\n')  # finish


def import_to_cashbook():
    config = Config(dotenv_values())

    # Get marker info
    marker = Marker(config.cash_book_path)

    # Get target files
    files = NotimonLogScan(
        notimon_log_path=config.notimon_log_path,
        marker=marker
    ).scan()

    files.sort(key=lambda entry: entry.path)

    # Read CSV records
    daily_records = NotimonLogRead(
        notimon_log_path=config.notimon_log_path,
        marker=marker
    ).read(files)

    importer = Importer(cash_book_path=config.cash_book_path)

    for date, records in daily_records.items():
        importer.import_to_cashbook(records)


if '__main__' == __name__:
    try:
        import_to_cashbook()
    except Exception as e:
        print(e, file=stderr)
        exit(1)
