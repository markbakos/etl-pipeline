import random
import string
from typing import Tuple

def rand_email() -> str:
    prefix = "".join(random.choices(string.ascii_lowercase + string.digits, k=10))
    domain = random.choice(["test.com", "mail.org", "asd.com", "gmail.com"])
    return f"{prefix}@{domain}"

def rand_amount() -> float:
    base = random.uniform(5, 300)
    return round(base, 2)

def chance(p: float) -> bool:
    return random.random() < p

def clamp_probs(insert_p: float, update_p: float, delete_p: float) -> Tuple[float, float, float]:
    total = insert_p + update_p + delete_p
    if total <= 0:
        raise ValueError("At least one of insert/update/delete must be >0")
    return insert_p / total, update_p / total, delete_p / total

