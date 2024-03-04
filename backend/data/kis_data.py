"""Responsible for classes defining non-model query-sets from another DB."""
import logging
from typing import Generator, Never, Any
from datetime import date
from collections import Counter
from itertools import chain
from django.core.cache import cache
from rest_framework.exceptions import ValidationError
from django.conf import settings
from django.db.utils import IntegrityError
from .psycopg_module import BaseConnectionDB
from .sql_queries import QuerySets
from .serializers import KISDataSerializer, KISTableSerializer, MainDataSerializer, AccumulativeDataSerializer
from .models import MainData, Profiles, AccumulationOfIncoming

creds = settings.DB_CREDS
logger = logging.getLogger('data.kis_data.DataForDMK')


class CleanData:
    """
    Use this base class for creating defined class objects with passed kwargs.

    When kwargs is passing - they are becoming a new attributes for this class object.
    Designed for creating list of class objects for processing by serializer.
    The attrs of each class object represents column name and its value as a key=value pair
    gotten from KIS DB as a stored info.

    Does not have anything methods.
    """

    def __init__(self, **kwargs):
        """
        Initialize an object with attributes based on key-value pairs provided as keyword arguments.

        :param kwargs: *dict*: Keyword arguments representing attribute names and values for the object.
        """
        for key, value in kwargs.items():
            setattr(self, key, value)


class KISData:
    """
    KISData class.

    This class facilitates the creation of a generator object for retrieving data from the PostgreSQL database
    using a series of queries specified in the query_sets attribute. Each query pair in query_sets is a list containing
    the data query and its corresponding column queries.

    Attributes:
      db_conn:  The connection module to the PostgreSQL database, BaseConnection instance.

      cursor:  The method to execute queries on the PostgreSQL database.

    Methods:
      - get_data_generator -> Generator: Create a generator object performing each query from queryset by request.
    """

    def __init__(self, query_sets):
        """
        Initialize an instance of KISData.

        :param query_sets: *list*: A list containing the data queries
          and queries of its columns. Each query pair is a list.
        """
        self.query_sets = query_sets
        self.db_conn = BaseConnectionDB(dbname=creds['dbname'],
                                        host=creds['host'],
                                        user=creds['user'],
                                        password=creds['password']
                                        )
        self.cursor = self.db_conn.execute_query

    def get_data_generator(self) -> Generator:
        """
        Create a generator object performing each query from queryset by request.

        Retrieves data from the database based on the specified query passed as a list into init method as arg.

        :return: *Generator*.
        """
        for dataset in self.query_sets:
            yield self.cursor(dataset)
        self.db_conn.close_connection()


class DataProcessing:
    """
    The base class from which other classes are inherited to provide required data by adding specific sets of queries.

    Attributes:
      kisdata_obj: (KISData): The KISData instance.
      Its method giving us generator. Needed for connection status checking.

    Methods:
      - error_check(dataset) -> bool: Check if the dataset contains an error.

      - filter_dataset(dataset, ind, value) -> list: Filter dataset based on index and value. Needed for data sorting.

      - count_dataset_total(dataset) -> int: Count the total number of rows in the dataset.
    """

    qs = QuerySets

    def __init__(self, kisdata_obj):
        """
        Initialize the DataProcessing instance.

        :param kisdata_obj: *KISData*: A generator providing datasets.
        """
        self.kisdata_obj = kisdata_obj

    @staticmethod
    def filter_dataset(dataset, ind, value) -> list:
        """
        Filter passed dataset based on index and value.

        :param dataset: *list*: The dataset to filter.
        :type dataset: list[tuple]
        :param ind: *int*: Index to filter on.
        :type ind: int
        :param value: *int, str*: Value to match in the filter.
        :type value: int or str
        :return: *list*: Filtered dataset.
        """
        return [row for row in dataset if row[ind] == value]

    @staticmethod
    def count_dataset_total(dataset):
        """
        Count the total number of rows in the dataset.

        :param dataset: *list*: The dataset to count.
        :type dataset: list[tuple]
        :return: *int*: Total number of rows.

        """
        return len(dataset)

    @staticmethod
    def slice_dataset(dataset, mapping) -> list[list]:
        """
        Gather all the split lines into one to separate it into data and fields.

        :param dataset: *list*: Dataset for processing.
        :type dataset: list[tuple]
        :param mapping: *dict*: Dictionary for matching Russian column names and English ones.
        :type mapping: dict[str, str]
        :return: *list*: List of list - first it is column names, second is calculated amount of patients.
        """
        # Creating 1 row inside dataset instead many.
        stacked_tuples_dataset = [tuple(chain.from_iterable(map(tuple, dataset)))]
        # Getting column names from stacked tuple of KIS data.
        ru_columns = list(stacked_tuples_dataset[0][::2])
        # Creating en columns for matching to KIS serializer fields.
        en_columns = [mapping[column] for column in ru_columns]
        # Created dataset manually as list.
        counted_pats = list(stacked_tuples_dataset[0][1::2])
        return [en_columns, counted_pats]

    @staticmethod
    def create_instance(columns, dataset) -> list[CleanData]:
        """
        Create instances of a target class with data retrieved from the database.

        :param columns: *list*: A list of column names representing the attributes of the `CleanData` instances.
        :type columns: list[str]
        :param dataset: *list*: A list of lists, where each inner list contains
         data corresponding to a row in the database.
        :type dataset: list[tuple]
        :return: *list[CleanData]*: A list of `CleanData` class instances, each instantiated with data from the provided dataset.
        """
        instances_list = [CleanData(**dict(zip(columns, row))) for row in dataset]
        return instances_list


class DataForDMK(DataProcessing):
    """
    Class for processing, collecting, and saving data to the DMK DB.

    This class extends the functionality of DataProcessing to handle the process
    of collecting, preparing, and saving data to the DMK database using a generator.

    Attributes:
      kisdata_obj: (KISData): The KISData instance.
      Its method giving us generator. Needed for connection status checking.

    Methods:
      - count_data(dataset, ind, value) -> list: Count total, positive, and negative amounts in the dataset.

      - get_arrived_data() -> dict: Get data related to arrivals.

      - get_signout_data() -> dict: Get data related to signouts and deaths.

      - get_reanimation_data() -> dict: Get data related to reanimation.

      - save_to_dmk(): Save the prepared data to the DMK DB using the MainData model and its serializer.
    """

    qs = QuerySets
    dmk_cols = qs.DMK_COLUMNS

    def __init__(self, kisdata_obj):
        """
        Initialize the DataForDMK instance, it inherited from parent class.

        :param kisdata_obj: *KISData*: A generator providing datasets.
        """
        super().__init__(kisdata_obj)

    def count_data(self, dataset, ind, value) -> list[int]:
        """
        Count total, positive, and negative amounts in the dataset.

        Negative means refused and deads, and positive means hospitalized and moved to other clinics.

        :param dataset: *list*: The dataset to count.
        :type dataset: list[tuple]
        :param ind: *int*: Index to count positive and negative amounts.
        :type ind: int
        :param value: *int, str*: Value to match in the filter.
        :type value: int or str
        :return: *list*: List containing total, positive, and negative amounts.
        """
        data = self.filter_dataset(dataset, ind, value)
        total_amount = self.count_dataset_total(dataset)
        positive_amount = len(data)
        negative_amount = total_amount - positive_amount
        if ind == 1:
            return [total_amount, negative_amount]
        return [total_amount, positive_amount, negative_amount]

    def get_arrived_data(self, arrived_dataset) -> dict:
        """
        Get data related to arrivals, hosp and refused patients.

        :return: *dict*: Dictionary containing arrived, hospitalized, and refused data.
        """
        result_keys = self.dmk_cols[0:3]
        ready_values = self.count_data(arrived_dataset, 0, 1)
        return dict(zip(result_keys, ready_values))

    def get_signout_data(self, signout_dataset) -> dict:
        """
        Get data related to signouts and deaths patients.

        :return: *dict*: Dictionary containing signout and deaths data.
        """
        result_keys = self.dmk_cols[3:5]
        ready_values = self.count_data(signout_dataset, 1, 'Другая причина')
        return dict(zip(result_keys, ready_values))

    def get_reanimation_data(self, reanimation_dataset) -> dict:
        """
        Get data related to reanimation patients.

        :return: *dict*: Dictionary containing reanimation data.
        """
        return {self.dmk_cols[-1]: self.count_dataset_total(reanimation_dataset)}

    def get_dept_hosps(self, dh_dataset):
        profiles_queryset = Profiles.objects.all()
        profiles = [{profile.name: profile.id} for profile in profiles_queryset]
        # Here we are checking profile name from each row given dataset so that it accords
        # profiles added into Profiles model and get list of ready to serializing dicts.
        result_dicts = [{'number': row[1], 'profile': o.get(row[0])}
                        for row in dh_dataset for o in profiles if o.get(row[0]) is not None]
        return result_dicts

    def __collect_data(self) -> dict:
        """
        Get calculated main values for detail boards on the front-end for saving to DMK DB.

        The method calculates data from a set of datasets obtained one by one through iteration of the generator
        and then concatenates it into one common dictionary.

        :return: *dict*: Main data for saving to DMK DB.
        :raises StopIteration: If the generator is already empty. This point also will writen to logs.
        """
        if self.kisdata_obj.db_conn.conn is None:
            main_data = {i: None for i in self.dmk_cols}
        else:
            gen = self.kisdata_obj.get_data_generator()
            arrived = self.get_arrived_data(next(gen))
            signout = self.get_signout_data(next(gen))
            deads = self.get_reanimation_data(next(gen))
            dh_dataset = self.get_dept_hosps(next(gen))
            main_data = arrived | signout | deads
        # Add dates key-value pair to collected data dict.
        today_dict = {'dates': date.today()}
        ready_main_data = today_dict | main_data
        return {'main_data': ready_main_data, 'accum_data': dh_dataset}

    @staticmethod
    def __check_data(data):
        """
        Check ready collected data and write log info depending on checking result.

         If they are contains None values, log warning about this.
         Otherwise, log info about successfully data inserting.

        :param data: *list*:
        :type data: list[dict]
        """
        for result in data:
            if None in [value for value in result.values()]:
                logger.warning('Data contains NULLs')
            else:
                logger.info('Data recorded successfully.')

    @staticmethod
    def __translate(err):
        """
        Replace original text error to english lang and return ready en string.

         It needed for correct symbols displaying in log file

        :param err: Error string as a some exception class instance.
        :return: *str*: Ready changed error text.
        """
        err_text = str(err)
        if err_text[2:7] == 'dates':
            err_text = err_text.replace("{'dates': [ErrorDetail(string='main data с таким dates"
                                        " уже существует.', code='unique')]}",
                                        "Can not write data to row with existing 'dates' value."
                                        " The 'date' column has a field constraint that the value is unique."
                                        )
        return err_text

    def save_to_dmk(self) -> Any:
        """
        Save the prepared data to the DMK DB using the MainData model and its serializer.

        :raises ValidationError: If the serializer validation fails.
        :raises SyntaxError: If there is a syntax error in the serializer.
        :raises AssertionError: If there is an assertion error during saving.

        :return: MainData instance and list of AccumulatedData instances:
         Saved data as a model instance. If on of the exception will raise - returns None and
         write to log file.
        """
        common_dict = self.__collect_data()
        main = common_dict['main_data']
        accum = common_dict['accum_data']
        main_res = self.save_main(main)
        accum_res = self.save_accumulated(accum)
        return [main_res, accum_res]

    def save_main(self, main_data: dict) -> Any:
        """
        Serializer and save a new model instance.

         If any defined errors will occur - instance will now save and method returns None.
        :param main_data: *dict*: Ready for serializing data.
        :return: *Any*
        """
        main_sr = MainDataSerializer(data=main_data)
        try:
            main_sr.is_valid(raise_exception=True)
            main_sr.save()
            return main_sr.save()
        except (ValidationError, SyntaxError, AssertionError, IntegrityError) as e:
            en_error = self.__translate(e)
            logger.error(en_error)

    def save_accumulated(self, accum_data: list[dict]) -> Any:
        """
        Iterate through given Serializer and save a few new model instances.

         If any defined errors will occur - instance will now save and method returns None.
        :param accum_data: *list*: List of dicts ready for serializing data.
        :return: *Any*
        """
        saved_instances = []
        for row in accum_data:
            accum_sr = AccumulativeDataSerializer(data=row)
            try:
                accum_sr.is_valid(raise_exception=True)
                accum_sr.save()
                saved_instances.append(accum_sr.save())
            except (ValidationError, SyntaxError, AssertionError, IntegrityError) as e:
                logger.error(e)
        return saved_instances


class KISDataProcessing(DataProcessing):
    """
    Class contains processing methods for KIS data.

    Used for getting and processing KIS DB data directly.

    Class attributes:
      querysets: QuerySets instance for getting access to its attrs and methods permanent.

    Attributes:
      kisdata_obj: (KISData): The KISData instance.
      Its method giving us generator. Needed for connection status checking.

    Methods:
      - __count_values(dataset, ind, keywords) -> list[int]: Count values in the dataset based on index and keywords.

      - __result_for_sr(columns, dataset) -> list[CleanData]: Create instances of CleanData with data from the dataset.

      - __serialize(ready_dataset) -> dict: Serialize the processed dataset using the KISDataSerializer.

      - __arrived_process() -> dict: Process and serialize data related to arrivals.

      - __dept_hosp_process() -> dict: Process and serialize data related to departmental hospitals.

      - __signout_process() -> dict: Process and serialize data related to signouts.

      - create_ready_dicts() -> list[dict]: Create a list of dictionaries containing processed and serialized datasets.
    """

    deads_oar = []
    counted_oar = []

    def __init__(self, kisdata_obj):
        """
        Initialize the KISDataProcessing instance.

        :param kisdata_obj: *Generator*: A generator providing datasets.
        """
        super().__init__(kisdata_obj)

    def __count_values(self, dataset, ind, keywords) -> list[int]:
        """
        Count values in the dataset based on index and keywords.

        :param dataset: *list*: The dataset to count.
        :type dataset: list[tuple]
        :param ind: *int*: Index to count values.
        :type ind: int
        :param keywords: *list*: Keywords to match in the filter.
        :type keywords: list[str]
        :return: *list[int]*: List containing counted values.
        """
        custom_filter = self.filter_dataset
        grouped_list = [len(custom_filter(dataset, ind, i)) for i in keywords]
        return grouped_list

    def __result_for_sr(self, columns, dataset) -> list[CleanData]:
        """
        Create instances of `CleanData` with data from the dataset.

        :param columns: *list*: A list of column names representing the attributes of the `CleanData` instances.
        :type columns: list[str]
        :param dataset: *list*: A list of lists, where each inner list contains
         data corresponding to a row in the database.
        :type dataset: list[tuple]
        :return: *list[CleanData]*: A list of `CleanData` class instances, each instantiated with
         data from the provided dataset.
        """
        return self.create_instance(columns, dataset)

    @staticmethod
    def __serialize(ready_dataset, data_serializer=True) -> dict:
        """
        Serialize the processed dataset using the KISDataSerializer.

        :param ready_dataset: *list[CleanData]*: The processed dataset.
        :type ready_dataset: list[CleanData]
        :return: *dict*: Serialized data.
        """
        if data_serializer:
            sr_data = KISDataSerializer(ready_dataset, many=True).data
        else:
            sr_data = KISTableSerializer(ready_dataset, many=True).data
        return sr_data

    def __arrived_process(self, arrived_dataset) -> dict:
        """
        Process and serialize data related to arrivals.

        :param arrived_dataset: *list*: Dataset from DB as a list of tuples.
        :type arrived_dataset: list[tuple]
        :return: *dict*: Serialized data.
        """
        # Defining columns for serializer and values for filtering datasets.
        columns, channels, statuses = self.qs.COLUMNS['arrived'], self.qs.channels, self.qs.statuses
        # Getting first dataset by generator.
        hosp_data = self.filter_dataset(arrived_dataset, 0, 1)
        # Calculating channels numbers.
        sorted_channels_datasets = self.__count_values(hosp_data, 2, channels)
        # Calculating patients statuses.
        sorted_statuses_datasets = self.__count_values(hosp_data, -1, statuses)
        # Creating 1 row data in dataset.
        summary_dataset = [tuple(sorted_channels_datasets+sorted_statuses_datasets)]
        ready_dataset = self.__result_for_sr(columns, summary_dataset)
        return self.__serialize(ready_dataset)

    def __signout_process(self, signout_dataset) -> dict:
        """
        Process and serialize data related to signouts.

        :param signout_dataset: *list*: Dataset from DB as a list of tuples.
        :type signout_dataset: list[tuple]
        :return: *dict*: Serialized data.
        """
        columns, keywords = self.qs.COLUMNS['signout'], self.qs.signout
        # Calculating signout from defined depts
        signout_only = self.filter_dataset(signout_dataset, 1, 'Выписан')
        counter = Counter(signout_only)
        counted_signout_dataset = [(dept[0], cnt) for dept, cnt in counter.items()]
        # Preparing data for creating processed dataset
        packed_data = self.slice_dataset(counted_signout_dataset, self.qs.depts_mapping)
        en_columns, dataset = packed_data[0], packed_data[1]
        sorted_signout_dataset = self.__count_values(signout_dataset, 1, keywords)
        summary_dataset = [tuple(sorted_signout_dataset + dataset)]
        summary_columns = columns + en_columns
        ready_dataset = self.__result_for_sr(summary_columns, summary_dataset)
        return self.__serialize(ready_dataset)

    def __deads_process(self, deads_dataset) -> dict:
        """
        Process and serialize data related to signouts.

        :param deads_dataset: *list*: Dataset from DB as a list of tuples.
        :type deads_dataset: list[tuple]
        :return: *dict*: Serialized data.
        """
        # Counting deads patients in OARs
        if oars_filtered := [len(self.filter_dataset(deads_dataset, 6, oar))
                             for oar in ['ОРИТ №1', 'ОРИТ №2', 'ОРИТ №3']]:
            self.deads_oar = [tuple(oars_filtered)]
        # Processing table data for serializing.
        columns = self.qs.COLUMNS['deads_t']
        ready_dataset = self.__result_for_sr(columns, deads_dataset)
        return self.__serialize(ready_dataset, data_serializer=False)

    def __oar_process(self, dataset, columns) -> dict:
        """
        Process and serialize data related to hospitalized in reanimation.

        :param dataset: *list*: Dataset from DB as a list of tuples.
        :type dataset: list[tuple]
        :param columns: *list*: List of column names for mapping with data.
        :type columns: list[str]
        :return: *dict*:
        """
        # Creating list of calculating lens of each separated datasets that filtered by oar number
        if oar_nums := [len(self.filter_dataset(dataset, 3, oar)) for oar in ['ОРИТ №1', 'ОРИТ №2', 'ОРИТ №3']]:
            self.counted_oar.append([tuple(oar_nums)])
        ready_dataset = self.__result_for_sr(columns, dataset)
        return self.__serialize(ready_dataset, data_serializer=False)

    def oar_count(self) -> list[dict]:
        """
        Count amount of needed patients related to reanimates.

        In process of processing data we are first saving calculated numbers in class attributes,
        then using it for serializing and including into summary answer list.

        :return: *list[dict]*: List of serialized data with keywords as a dict.
        """
        oar_columns = self.qs.COLUMNS['oar_amounts']
        living_list = [(self.__result_for_sr(oar_columns, i)) for i in self.counted_oar]
        deads_list = self.__result_for_sr(oar_columns, self.deads_oar)
        result = [{'arrived_nums': self.__serialize(living_list[0])},
                  {'moved_nums': self.__serialize(living_list[1])},
                  {'current_nums': self.__serialize(living_list[2])},
                  {'deads_nums': self.__serialize(deads_list)}
                  ]
        return result

    def create_ready_dicts(self) -> list[dict]:
        """
        Create an ordered list of dictionaries containing processed and serialized datasets.

         Checking for connection failure, if so, then manually creating dictations with None values
         and returning them as serialized data for the response.

        :return: *list[dict]*: List of dictionaries containing processed and serialized datasets.
        """
        keywords = self.qs.DICT_KEYWORDS
        if self.kisdata_obj.db_conn.conn is None:
            ready_dataset = [None for i in range(8)]
            result = [dict(zip(keywords, ready_dataset))]
            return result
        # If connection successfully getting and processing data.
        gen = self.kisdata_obj.get_data_generator()
        arrived = self.__arrived_process(next(gen))
        # hosp_dept = self.__dept_hosp_process(next(gen))
        signout = self.__signout_process(next(gen))
        deads = self.__deads_process(next(gen))
        oar_arrived = self.__oar_process(next(gen), self.qs.COLUMNS['oar_arrived_t'])
        oar_moved = self.__oar_process(next(gen), self.qs.COLUMNS['oar_moved_t'])
        oar_current = self.__oar_process(next(gen), self.qs.COLUMNS['oar_current_t'])
        oar_numbers = self.oar_count()
        # Creating list of ready processed datasets.
        ready_dataset = [arrived, signout, deads, oar_arrived, oar_moved, oar_current, oar_numbers]
        # Creating list of dicts where keys takes from query class
        # and values are ready dataset iterating list of them one by one.
        result = [{keywords[ready_dataset.index(dataset)]: dataset for dataset in ready_dataset}]
        return result


def ensure_cashing() -> None:
    """
    Check redis cash and write data into if storage is empty.

     We are calling this func in the main view to avoid unnecessary calls to the database.
     First call of the day provides writing data into cache and use
     the cache during the day new call to db instead.
    """
    today = date.strftime(date.today(), '%Y-%m-%d')
    if cache.get('dmk') is None:
        main_dmk = MainDataSerializer(MainData.objects.custom_filter(), many=True).data
        accum_dmk = AccumulativeDataSerializer(
                    AccumulationOfIncoming.objects
                    .select_related().filter(dates=today)
                    .values('id', 'dates', 'number', 'profile__name'),
                    many=True
                    ).data
        dmk = {'main': main_dmk, 'accum_dmk': accum_dmk}
        cache.set('dmk', dmk)
    if cache.get('kis') is None:
        p_kis = KISDataProcessing(KISData(QuerySets().queryset_for_kis())).create_ready_dicts()
        cache.set('kis', p_kis)

