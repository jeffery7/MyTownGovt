#!/usr/bin/env python3

from datetime import datetime
from collections import defaultdict

class ErrorTracker:
    def __init__(self):
        self.errors = []
        self.error_counts = defaultdict(int)
        self.total_errors = 0
        self.total_warnings = 0

    def add_error(self, error_type, message, url, retry_count=None, is_warning=False):
        error = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'type': error_type,
            'message': message,
            'url': url,
            'is_warning': is_warning
        }
        if retry_count is not None:
            error['retry_count'] = retry_count
        
        self.errors.append(error)
        self.error_counts[error_type] += 1
        if is_warning:
            self.total_warnings += 1
        else:
            self.total_errors += 1