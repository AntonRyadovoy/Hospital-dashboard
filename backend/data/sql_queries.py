"""This module defines class responsible for giving whole query set of separated queries."""
from datetime import date


class QuerySets:
    """
    QuerySets class.

    Contains specific database queries and columns for receiving data from that queries as class attributes,
    as well as filter words, column sets as dicts for mapping, and serializer keywords.
    Also has method for creating common list of these queries.
    """

    today = date.today

    ARRIVED = f"""
               SELECT
               	  1 AS Госпитизировано,
               	  mm.dept_get_name(h.dept_id) AS Отделение,
               	  ect.dzm56 AS Канал_госпитализации,
               		CASE h.patienttype
               			WHEN '0' THEN 'ЗЛ'
               			WHEN '1' THEN 'Иногородний'
               			WHEN '2' THEN 'НР'
               			WHEN '3' THEN 'НИЛ'
               			WHEN '4' THEN 'Контрагент'
               			WHEN '5' THEN 'ДМС'
               			WHEN '-1' THEN 'Не указано'
               		ELSE h.patienttype::TEXT
               		END AS Тип_пациента
               
               FROM mm.mdoc m
               JOIN mm.hospdoc h ON h.mdoc_id = m.id
               JOIN mm.ehr_case ec ON ec.id = h.ehr_case_id
               JOIN mm.ehr_case_title ect ON ect.caseid  = ec.id 
               
               WHERE ec.create_dt BETWEEN current_date - INTERVAL '1 day, -7 hours' AND current_date - INTERVAL '-7 hours'
               
               UNION ALL 									
               
               SELECT 
                      0 AS отказано,
               	   'Приемное' AS Отделение,
               	    ect2.dzm56 AS Канал_госпитализации,
               		  CASE a.patienttype
               			WHEN '0' THEN 'ЗЛ'
               			WHEN '1' THEN 'Иногородний'
               			WHEN '2' THEN 'НР'
               			WHEN '3' THEN 'НИЛ'
               			WHEN '4' THEN 'Контрагент'
               			WHEN '5' THEN 'ДМС'
               			WHEN '-1' THEN 'Не указано'
               		ELSE a.patienttype::TEXT
               		END AS Тип_пациента
               		FROM mm.ambticket a
               		JOIN mm.hosp_refuse hr ON hr.ambticket_id = a.id 
               		JOIN mm.ehr_case_title ect2 ON ect2.caseid  = a.ehr_case_id
               
               WHERE hr.end_dt BETWEEN current_date - INTERVAL '1 day, -7 hours' AND current_date - INTERVAL '-7 hours';
               """

    DEPT_HOSP = """SELECT med_profile, amount FROM mm.dept_hosp;"""

    SIGNOUT = f"""
               SELECT 
               sg.dept,
               sg.status
               FROM mm.signout sg
               WHERE DATE(dates) = '{today()}';  
               """

    DEADS = """SELECT pat_fio, ib_num, sex, agee, arriving_dt, state, dept, days, diag_arr, diag_dead FROM mm.deads;"""

    OAR_ARRIVED_QUERY = """SELECT pat_fio, ib_num, ages, dept, doc_fio, diag_start FROM mm.oar_arrived;"""

    OAR_MOVED_QUERY = """SELECT pat_fio, ib_num, ages, dept, doc_fio, move_date, from_dept, diag_start
                         FROM mm.oar_moved;"""

    OAR_CURRENT_QUERY = """SELECT pat_fio, ib_num, ages, dept, doc_fio, days, diag_start FROM mm.oar_current;"""

    # Each string of this list is a keyword of dict where value is a serialized data.
    DICT_KEYWORDS = ['arrived', 'signout', 'deads',
                     'oar_arrived', 'oar_moved', 'oar_current', 'oar_numbers']

    # Lists of columns for mapping with values to creating CleanData class instances.
    COLUMNS = {
        'arrived': ['ch103', 'clinic_only', 'ch103_clinic', 'singly', 'ZL', 'foreign', 'moscow', 'undefined'], # Add plan field
        'signout': ['deads', 'moved', 'signout'],
        'deads_t': ['pat_fio', 'ib_num', 'sex', 'age', 'arriving_dt', 'state', 'dept', 'days', 'diag_arr', 'diag_dead'],
        'oar_arrived_t': ['pat_fio', 'ib_num', 'age', 'dept', 'doc_fio', 'diag_start'],
        'oar_moved_t': ['pat_fio', 'ib_num', 'age', 'dept', 'doc_fio', 'move_date', 'from_dept', 'diag_start'],
        'oar_current_t': ['pat_fio', 'ib_num', 'age', 'dept', 'doc_fio', 'days', 'diag_start'],
        'oar_amounts': ['oar1', 'oar2', 'oar3']
    }

    DMK_COLUMNS = ['arrived', 'hosp', 'refused', 'signout', 'deads', 'reanimation']

    # Filter-words for filter_dataset method of DataProcessing class.
    channels = ['103', 'Поликлиника', '103 Поликлиника', 'самотек']  # Add plan field
    statuses = ['ЗЛ', 'Иногородний', 'ДМС', 'Не указано']  # Add plan field
    signout = ['Умер', 'Переведен', 'Выписан']
    # Dict for mapping with serializer fields (relates to "план/факт по профилям" table).
    # All english names is fields of serializer.
    profiles_mapping = {
        'Терапия': 'therapy',
        'Хирургия': 'surgery',
        'Кардиология': 'cardiology',
        'Урология': 'urology',
        'Неврология': 'neurology'
    }

    # Dict for mapping columns on russian language with serializer fields (relates to "выписанные по отделениям" table).
    # All english names is fields of serializer.
    depts_mapping = {
        'ОРИТ №1': 'oar1',
        'ОРИТ №2': 'oar2',
        'ОРИТ №3': 'oar3',
        'Кардиологическое отделение': 'cardio_d',
        'Хирургическое отделение': 'surgery_d',
        'Терапевтическое отделение': 'therapy_d'
    }

    def queryset_for_dmk(self):
        """
        Create list of lists queries from class attributes needed for data to DMK DB.

        :return: List of lists.
        """
        result = [self.ARRIVED, self.SIGNOUT, self.OAR_ARRIVED_QUERY, self.DEPT_HOSP]
        return result

    def queryset_for_kis(self):
        """
        Create list of lists queries from class attributes needed for data to front-end.

        :return: *list*: List of lists.
        """
        dmk_queries = self.queryset_for_dmk()[:-1]
        dmk_queries.insert(-1, self.DEADS)
        result = dmk_queries + [self.OAR_MOVED_QUERY, self.OAR_CURRENT_QUERY]
        return result

    def chosen_date_query(self, query: str, chosen_date: str) -> list:
        """
        Replace date in the given query to passed and return query with needed date.

        :param query: *str*: Original class attribute query contains today date filter.
        :param chosen_date: Date for filtering that was chose users.
        :return: *str*: Changed query contains actual chosen date.
        """
        new_query = query.replace(str(self.today()), chosen_date)
        return [new_query]

