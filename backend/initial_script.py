"""
Main script that allows collect data and write into DMK DB when start using app.
Useful if app starts not the first day of month.
"""
from django.db import connection
from data.kis_data import QuerySets, KISData, DataForDMK, dates_period
from data.caching import Cacher


class Initializer:
    """
    Initializes data collection and writing into the DMK database.

    Usage:
        - from initial_script import Initializer
        - initializer = Initializer()
        - initializer.cache_initialized()

    It will collect and write data into the database and also put them into Redis cache.
    If you want to collect data for a specific period (constrained by 1 month), pass the number of days
    into the initializer instance:

        - from initial_script import Initializer
        - initializer = Initializer(7)
        - initializer.cache_initialized()
    """

    queries = QuerySets()

    def __init__(self, period: int = 0):
        """
        Initializes the Initializer object with an optional period parameter.

        :param period: Number of days to collect data for. Defaults to 0 for today's only data.
        """
        self.period = period

    def initialize_today(self):
        """Initializes data collection and writing for today's data."""
        dmk_query = self.queries.queryset_for_dmk()
        kis_datasets = KISData(dmk_query)
        DataForDMK(kis_datasets).save_to_dmk()

    def initialize_period(self):
        """Initializes data collection and writing for a specified period of days."""
        id_cnt = 0
        dates = dates_period(self.period)
        cursor = connection.cursor()
        for date in dates:
            dmk_query = self.queries.chosen_date_query(self.queries.queryset_for_dmk(), date)
            dept_hosp_query = self.queries.chosen_date_query(self.queries.DEPT_HOSP, date)

            kis_datasets = KISData(dmk_query)
            dh_kis_datasets = KISData(dept_hosp_query)

            main_dmk_instance = DataForDMK(kis_datasets)
            main_data = main_dmk_instance.collect_data(chosen_date=date)['main_data']
            main_dmk_instance.save_main(main_data)

            dh_generator = dh_kis_datasets.get_data_generator()
            dh_raw_dataset = DataForDMK(dh_kis_datasets).get_dept_hosps(next(dh_generator), True)
            # Writting accumulated data
            for row in dh_raw_dataset:
                raw_query = self.queries.insert_accum_query(row, date, id_cnt)
                cursor.execute(raw_query)
                id_cnt += 1

    def cache_initialized(self):
        """
        Initializes data collection and caching.

        :return: True if caching is successful.
        """
        self.initialize_period() if self.period else self.initialize_today()
        Cacher().main_caching()
        return True
