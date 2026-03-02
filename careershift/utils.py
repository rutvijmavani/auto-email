# careershift/utils.py — Utility helpers for CareerShift automation

import time
import random


def human_delay(min_sec=1.0, max_sec=3.0):
    """Sleep for a random duration to mimic human behavior."""
    time.sleep(random.uniform(min_sec, max_sec))


def slow_type(element, text):
    """Type text character by character with random delays."""
    for char in text:
        element.type(char)
        time.sleep(random.uniform(0.05, 0.18))