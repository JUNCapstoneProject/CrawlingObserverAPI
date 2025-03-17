import random
import time

# 요청 사이 랜덤 딜레이 적용
def random_delay():
    interval = round(random.uniform(1, 2), 2)
    time.sleep(interval)
    return interval