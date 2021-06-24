import time


def fahrenheit_to_celcius(temp):
    """
    Converts a temperature in farenheit to celcius
    :param temp: temperature in farenheit, float
    :return: temperature in degrees, float
    """
    return (temp - 32) * 5 / 9


def display_eta(start_time, pct_progress):
    """ Give an ETA and display it every threshold_step%"""

    # 1. How much time it took until now:
    current_time_taken = time.time() - start_time
    time_to_do_100_pct_min = (current_time_taken * 100 / pct_progress) / 60
    # 2. Time to do the remaining pct
    remaining_pct = 100 - pct_progress
    eta_min = time_to_do_100_pct_min * remaining_pct / 100

    print(f'{round(pct_progress, 2)}% --- Estimated Time Remaining: {round(eta_min)} minutes')
