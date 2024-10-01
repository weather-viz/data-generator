from datetime import datetime, timedelta

def is_leap_year(year):
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

def day_of_year(date):
    try:
        date = date.replace(year=2001)
        return date.timetuple().tm_yday - 1
    except ValueError:
        return None

def split_by_days(data):
    start_date, end_date = data[0][0], data[-1][0]
    years = [ i for i in range(start_date.year, end_date.year + 1)]

    base = datetime(2001,1,1)
    days = [ base+timedelta(days=d) for d in range(365)]
    days = [ f"{d.month:02}-{d.day:02}" for d in days ]

    result = [[None for _ in years] for _ in days]

    for (date, value) in data:
        year_index = years.index(date.year)
        day_index = day_of_year(date)
        if day_index is not None:
            result[day_index][year_index] = value

    return result, years, days

def split_by_weeks(data):
    pass

def split_by_months(data):
    pass