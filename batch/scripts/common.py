def cast_date(datetime: str):
    return "DATE('{}')".format(datetime)


def cast_datetime(datetime: str):
    return "CAST('{}' AS DATETIME)".format(datetime)
