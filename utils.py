def is_round_number(s: str) -> bool:
    try:
        num = float(s)
        return num.is_integer()
    except ValueError:
        # If conversion fails, it means it's not a valid number
        return False