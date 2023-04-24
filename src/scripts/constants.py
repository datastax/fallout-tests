"""
This Python file contains all constants used throughout the hunter_csv application.
"""

# Fixed directories
CASSANDRA_PROJ_DIR = '/home/ec2-user/cassandra'
FALLOUT_TESTS_PROJ_DIR = '/home/ec2-user/fallout-tests'
FALLOUT_TESTS_SHA_PROJ_DIR = '/home/ec2-user/fallout-tests_sha'
HUNTER_CSV_PROJ_DIR = '/home/ec2-user/hunter_csv'
NIGHTLY_RESULTS_DIR = '/home/ec2-user/nightly_results'

# Date directory-related regex pattern
DATE_DIR_REGEX_PATTERN = r'\d{4}_\d{2}_\d{2}'

# Commit-related column names
CASSANDRA_COL_NAME = 'commit'  # Name as expected by Hunter
FALLOUT_TESTS_COL_NAME = 'fallout_tests_commit'

# Time and date formats
FMT_TIME = '%H:%M:%S'
FMT_Y_D_M = '%Y_%d_%m'
FMT_Y_M_D = '%Y_%m_%d'

# Hunter csv file names and associated LWT tests-related constants
HUNTER_FILE_FMT = '.csv'
HUNTER_PREFIX = 'hunter-'
TWO_GIT_SHA_SUFFIX = '-w-two-git-shas'
FIXED_100_CSV_NAME = f"{'hunter-lwt-fixed-100-partitions'}{TWO_GIT_SHA_SUFFIX}"
RATED_100_CSV_NAME = f"{'hunter-lwt-rated-100-partitions'}{TWO_GIT_SHA_SUFFIX}"
FIXED_1000_CSV_NAME = f"{'hunter-lwt-fixed-1000-partitions'}{TWO_GIT_SHA_SUFFIX}"
RATED_1000_CSV_NAME = f"{'hunter-lwt-rated-1000-partitions'}{TWO_GIT_SHA_SUFFIX}"
FIXED_10000_CSV_NAME = f"{'hunter-lwt-fixed-10000-partitions'}{TWO_GIT_SHA_SUFFIX}"
RATED_10000_CSV_NAME = f"{'hunter-lwt-rated-10000-partitions'}{TWO_GIT_SHA_SUFFIX}"
LIST_OF_CSV_NAMES = [
    FIXED_100_CSV_NAME, RATED_100_CSV_NAME, FIXED_1000_CSV_NAME,
    RATED_1000_CSV_NAME, FIXED_10000_CSV_NAME, RATED_10000_CSV_NAME
]

LWT_TESTS_NAMES = [
    'lwt-fixed-100-partitions', 'lwt-fixed-1000-partitions', 'lwt-fixed-10000-partitions',
    'lwt-rated-100-partitions', 'lwt-rated-1000-partitions', 'lwt-rated-10000-partitions'
]

SUBSTR_TESTS_NAMES = [
    '-fixed-100-', '-rated-100-', '-fixed-1000-',
    '-rated-1000-', '-fixed-10000-', '-rated-10000-'
]

LWT_TEST_RUN_EXEC_TIME = '23:00:00'

PROSPECTIVE_MODE = True

# Metrics' columns for Hunter
LIST_OF_COLS_TO_EXTRACT = [
    'Total Operations',
    'Op Rate',
    'Min Latency',
    'Avg Latency',
    'Median Latency',
    '95th Latency',
    '99th Latency',
    '99.9th Latency',
    'Max Latency',
    'Median Absolute Deviation',
    'Interquartile Range'
]

DICT_OF_RENAMED_COLS = {
    LIST_OF_COLS_TO_EXTRACT[0]: 'totalOps',
    LIST_OF_COLS_TO_EXTRACT[1]: 'opRate',
    LIST_OF_COLS_TO_EXTRACT[2]: 'minLat',
    LIST_OF_COLS_TO_EXTRACT[3]: 'avgLat',
    LIST_OF_COLS_TO_EXTRACT[4]: 'medianLat',
    LIST_OF_COLS_TO_EXTRACT[5]: 'p95',
    LIST_OF_COLS_TO_EXTRACT[6]: 'p99',
    LIST_OF_COLS_TO_EXTRACT[7]: 'p99.9',
    LIST_OF_COLS_TO_EXTRACT[8]: 'maxLat',
    LIST_OF_COLS_TO_EXTRACT[9]: 'MAD',
    LIST_OF_COLS_TO_EXTRACT[10]: 'IQR'
}

# By default, fetching a Git sha of a repo (e.g., Cassandra or fallout-tests) from a log file
USE_LOG_FILE = True

# Types of performance tests currently supported
TUPLE_SUPPORTED_TESTS = ('lwt-',)
