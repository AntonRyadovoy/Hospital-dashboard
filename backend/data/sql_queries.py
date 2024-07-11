"""This module defines class responsible for giving whole query set of separated queries."""
from datetime import date
from typing import Union


class QuerySets:
    """
    QuerySets class.

    Contains specific database queries and columns for receiving data from that queries as class attributes,
    as well as filter words, column sets as dicts for mapping, and serializer keywords.
    Also has method for creating common list of these queries.
    """

    today = date.today

    KIS_PROFILES = """
                   SELECT id, name
                   FROM mm.profile_med;
                   """

    ARRIVED = """
              SELECT 
              ar.status,
              ar.dept,
              ar.channel,
              ar.patient_type 
              FROM mm.arrived ar
              WHERE dates BETWEEN CURRENT_DATE - INTERVAL '18 hours' and CURRENT_DATE + INTERVAL '6 hours';
              """

    DEPT_HOSP = """
                SELECT profile_id, amount FROM mm.dept_hosp
                WHERE dates BETWEEN CURRENT_DATE - INTERVAL '18 hours' and CURRENT_DATE + INTERVAL '6 hours';
                """

    SIGNOUT = """
              SELECT 
              sg.dept,
              sg.status
              FROM mm.signout sg
              WHERE dates BETWEEN CURRENT_DATE - INTERVAL '18 hours' and CURRENT_DATE + INTERVAL '6 hours';
              """

    DEADS = """
            SELECT 
            dd.pat_fio,
            dd.ib_num,
            dd.sex,
            dd.agee,
            dd.arriving_dt,
            dd.state,
            dd.dept,
            dd.days,
            dd.diag_arr,
            dd.diag_dead
            FROM mm.deads dd
            WHERE dates BETWEEN CURRENT_DATE - INTERVAL '18 hours' and CURRENT_DATE + INTERVAL '6 hours';
            """

    OAR_ARRIVED_QUERY = """
                        SELECT 
                        pat_fio,
                        ib_num, 
                        ages, 
                        dept,
                        doc_fio, 
                        diag_start FROM mm.oar_arrived
                        WHERE dates BETWEEN CURRENT_DATE - INTERVAL '18 hours' and CURRENT_DATE + INTERVAL '6 hours';
                        """

    OAR_MOVED_QUERY = f"""
                       SELECT 
                       	    mm.famaly_io(m.surname,m.name,m.patron) AS ФИО_Пациента,
                            m.num||'-'||m.YEAR AS №ИБ,
                       	   EXTRACT(YEAR from age(m.beg_dt, p.birth)) as Возраст,
                       	   mm.dept_get_name(h.dept_id) AS Отделение,
                       	   mm.emp_get_fio_by_id (h.doctor_emp_id) AS Лечащий_врач,
                       	   h.dept_dt AS Дата_перевода,   
                       (SELECT d.name 
                       		FROM mm.hosp_move_hist hmh
                            	JOIN mm.dept d ON d.id = hmh.dept_id
                       	   	WHERE h.dept_dt = hmh.out_dt) AS Из_отделения,   
                       (SELECT ic.kod
                       		FROM mm.ds ds
                               JOIN mm.icd10 ic ON ic.id = ds.icd10_id
                               WHERE ds.ds_type_id = 1
                               AND ds.ehr_case_id = h.ehr_case_id
                               ORDER BY ds.create_dt DESC
                               LIMIT 1) AS Диаг_поступление   
                       FROM mm.mdoc m
                       JOIN mm.hospdoc h ON h.mdoc_id = m.id
                       JOIN mm.people p ON p.id = m.people_id

                       WHERE h.hosp_dt <=current_date - INTERVAL '1 day, -6 hours'
                       AND h.dept_dt BETWEEN current_date - INTERVAL '1 day, -6 hours' AND current_date - INTERVAL '-6 hours'
                       AND h.dept_id IN (SELECT d.id from mm.dept d WHERE d.dept_med_type_id = 10220)

                       ORDER BY h.dept_id DESC;
                       """

    OAR_CURRENT_QUERY = f"""
                         SELECT 
                         	   mm.famaly_io(m.surname,m.name,m.patron) AS ФИО_Пациента,
                               m.num||'-'||m.YEAR AS №ИБ,
                         	   EXTRACT(YEAR from age(m.beg_dt, p.birth)) as Возраст,
                         	   mm.dept_get_name(h.dept_id) AS Отделение,
                         	   mm.emp_get_fio_by_id (h.doctor_emp_id) AS Лечащий_врач,
                         	   h.bed_days AS койко_дни,
 
                         	   CASE WHEN EXISTS 
                         	    (SELECT ic.kod
                                    FROM mm.ds ds
                                    JOIN mm.icd10 ic ON ic.id = ds.icd10_id
                                    WHERE ds.ehr_case_id = h.ehr_case_id
                                    AND ds.ds_kind_id IN ('1') 
                                    AND ds.ds_type_id IN ('3')
                                    AND ds.create_dt  = (SELECT  max(d2.create_dt) 
                                     					  FROM mm.ds d2 
                                     					 WHERE d2.mdoc_id = m.id
                                     					   AND d2.ds_kind_id IN ('1') 
                                    						   AND d2.ds_type_id IN ('3'))
                                    						   LIMIT 1 )
                                 THEN (SELECT ic.kod
                         		         FROM mm.ds ds1
                         		         JOIN mm.icd10 ic ON ic.id = ds1.icd10_id
                         		         WHERE ds1.ehr_case_id = h.ehr_case_id
                         				 	AND ds1.ds_kind_id IN ('1')
                         		            AND ds1.ds_type_id IN ('3')
                         		            AND ds1.create_dt  = (SELECT  max(d3.create_dt) 
                                     					           FROM mm.ds d3 
                                     					           WHERE d3.mdoc_id = m.id
                                     					             AND d3.ds_kind_id IN ('1') 
                                    						             AND d3.ds_type_id IN ('3'))
                                    						             LIMIT 1 ) 
                                 ELSE  'не установлен' 
                         	    END AS Диаг_поступление
 
                         FROM mm.mdoc m
                         JOIN mm.hospdoc h ON h.mdoc_id = m.id
                         JOIN mm.people p ON p.id = m.people_id
 
                         WHERE h.leave_dt ISNULL
                         AND h.dept_id IN (SELECT d.id from mm.dept d WHERE d.dept_med_type_id = 10220);
                         """

    # Each string of this list is a keyword of dict where value is a serialized data.
    DICT_KEYWORDS = ['arrived', 'signout', 'deads', 'oar_deads',
                     'oar_arrived', 'oar_moved', 'oar_current', 'oar_numbers']

    # Lists of columns for mapping with values to creating CleanData class instances.
    COLUMNS = {
        'arrived': ['ch103', 'clinic_only', 'ch103_clinic', 'singly', 'plan',
                    'ZL', 'foreign', 'nr', 'nil', 'dms', 'undefined'],
        'signout': ['deads', 'moved', 'signout'],
        'deads_t': ['pat_fio', 'ib_num', 'sex', 'age', 'arriving_dt', 'state', 'dept', 'days', 'diag_arr', 'diag_dead'],
        'oar_arrived_t': ['pat_fio', 'ib_num', 'age', 'dept', 'doc_fio', 'diag_start'],
        'oar_moved_t': ['pat_fio', 'ib_num', 'age', 'dept', 'doc_fio', 'move_date', 'from_dept', 'diag_start'],
        'oar_current_t': ['pat_fio', 'ib_num', 'age', 'dept', 'doc_fio', 'days', 'diag_start'],
        'oar_amounts': ['oar1_d', 'oar2_d', 'oaronmk_d','oaroim_d','oar_d']
    }

    DMK_COLUMNS = ['arrived', 'hosp', 'refused', 'signout', 'deads', 'reanimation']
    DMK_DETAILS_COLUMNS = ('registered',)

    # Filter-words for filter_dataset method of DataProcessing class.
    # If needed to add something more - "aapend" it, e.g. insert at the end of existing matched list.
    channels = ['103', 'Поликлиника', '103 Поликлиника', 'самотек', 'план']
    statuses = ['ЗЛ', 'Иногородний', 'НР', 'НИЛ', 'ДМС', 'Не указано']
    signout = ['Умер', 'Переведен', 'Выписан']
    oar_depts = ['ОРИТ №1', 'ОРИТ №2', 'ОРИТ №3']

    # Dict for mapping columns on russian language with serializer fields (relates to "выписанные по отделениям" table).
    # All english names is fields of serializer.
    depts_mapping = {
        'Отделение реанимации и интенсивной терапии для больных с ОНМК': 'oaronmk_d',
        'Хирургическое отделение': 'surgery_d',
        'Отделение реанимации и интенсивной терапии № 1': 'oar1_d',
        'Приемное ДП': 'dp_d',
        'Отделение анестезиологии-реанимации': 'oar_d',
        'Отделение травматологии и ортопедии': 'trauma_d',
        'Нейрохирургическое отделение': 'neurosurgery_d',
        'Отделение реанимации и интенсивной терапии для больных с острым инфарктом миокарда': 'oaroim_d',
        'Отделение реанимации и интенсивной терапии № 2': 'oar2_d',
        'Кардиологическое отделение': 'cardio_d',
        'Терапевтическое отделение': 'therapy_d',
        'Эндокринологическое отделение': 'endo_d',
        'Неврологическое отделение для больных с ОНМК': 'neuroonmk_d',
        'Урологическое отделение': 'urology_d',
        'Отделение гнойной хирургии': 'pursurgery_d',
        '2 кардиологическое (ОИМ)': 'cardio2_d',
        'Стационар кратковременного пребывания': 'skp_d', 
        'Гинекологическое отделение': 'gynecology_d',
        'Приемное отделение': 'emer_d',
        'Многопрофильное отделение по оказанию платных медицинских услуг': 'multi_pay_d', 
        'Дневной стационар АПЦ': 'apc_d',
        'Отделение сочетанной травмы': 'combine_d',
        'Пульмонологическое отделение': 'pulmonology_d'
    }

    def queryset_for_dmk(self) -> list[str]:
        """
        Create list of lists queries from class attributes needed for data to DMK DB.

        :return: List of lists.
        """
        result = [self.ARRIVED, self.SIGNOUT, self.OAR_ARRIVED_QUERY, self.DEPT_HOSP]
        return result

    def queryset_for_kis(self) -> list[str]:
        """
        Create list of lists queries from class attributes needed for data to front-end.

        :return: *list*: List of lists.
        """
        dmk_queries = self.queryset_for_dmk()[:-1]
        dmk_queries.insert(-1, self.DEADS)
        result = dmk_queries + [self.OAR_MOVED_QUERY, self.OAR_CURRENT_QUERY]
        return result

    @staticmethod
    def chosen_date_query(queryset: Union[str, list], chosen_date: str) -> list:
        """
        Replace date in the given query to passed and return query with needed date.

        :param queryset: *str*: Original class attribute query contains today date filter.
        :param chosen_date: Date for filtering that was chose users.
        :return: *str*: Changed query contains actual chosen date.
        """
        if type(queryset) is list:
            new_query = [query.replace('CURRENT_DATE', f'DATE \'{chosen_date}\'') for query in queryset]
            return new_query
        new_query = queryset.replace('CURRENT_DATE', f'DATE \'{chosen_date}\'')
        return [new_query]

    @staticmethod
    def insert_accum_query(dataset, dates, id_cnt) -> str:
        profile_id, number = dataset[0], dataset[1]
        raw_query = f"""
                     INSERT INTO public.data_accumulationofincoming (id, dates, number, profile_id) 
                     VALUES 
                     ({id_cnt}, '{dates}', {number}, '{profile_id}');
                     """
        return raw_query


class EmergencyQueries:

    WAITINGS = """
               SELECT pat_fio, ib_num, dept, waiting_time, doc_fio from mm.waitings;
               """

    TOTAL_REFUSE = """
                   SELECT doc_fio, total_refuse FROM mm.total_refuse;
                   """

    __DETAIL_REFUSE = """
                      SELECT pat_fio, ib_num, diag, refuse_reason, refuse_date, doc_fio
                      FROM mm.refuse WHERE doc_fio = 'passed_doc_fio';
                      """

    COLUMNS = {
        'total_refuse': ['doc_fio', 'refuses_amount'],
        'detail_refuse': ['pat_fio', 'ib_num', 'diag', 'refuse_reason', 'refuse_date', 'doc_fio'],
        'waitings': ['pat_fio', 'ib_num', 'dept', 'waiting_time', 'doc_fio'],
    }

    def get_emergency_queries(self) -> list[str]:
        queries_list = [self.WAITINGS, self.TOTAL_REFUSE]
        return queries_list

    def get_detail_refuse_query(self, doc_names: list) -> list[str]:
        refuse_query = [self.__DETAIL_REFUSE.replace('passed_doc_fio', f'{name}') for name in doc_names]
        return refuse_query


class PlanHospitalizationQueries:

    PLAN_HOSP = """SELECT week, dates, dept, cnt_plan FROM mm.plan_hosp ORDER BY dates asc;"""

    COLUMNS = {
        'common_field': ['week', 'dates', 'dept', 'cnt_plan']
    }

    def get_plan_hosp_query(self) -> list[str]:
        return [self.PLAN_HOSP]
