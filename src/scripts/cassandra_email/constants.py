"""
This Python file contains all constants used to send an email with results from hunter on detected performance
regressions.
"""

# Constants for boto3 to connect to AWS Secret Manager
REGION_NAME = '<REGION_NAME>'
SECRET_NAME = '<SECRET_NAME>'

# Thresholds for sending email
THRESH_PERF_REGRESS = 11  # Determined based on retro analysis on Apr 16th, 2023

# Filenames for sending email
TXT_FILE_W_MSG = 'email_report.txt'

# Set boolean flag below to true to consider all bad significant changes detected by hunter; if False, only
# consider those that are highly significant (whose % change is beyond +/- THRESH_PERF_REGRESS %).
ALL_BAD_SIGNIF_CHANGES = False

# Template email message
TEMPLATE_MSG = 'Hello,\n\nPlease find the performance regressions detected ' \
               'by hunter as follows:\n\n\n\nBest regards,\n\nMarianne'

# Receiver's email address
RECEIVER_EMAIL = '<EMAIL_ADDRESS>'
