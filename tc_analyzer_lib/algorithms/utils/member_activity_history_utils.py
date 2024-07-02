import logging
from datetime import datetime, timedelta
from typing import Any

from numpy import array
from tc_analyzer_lib.DB_operations.mongodb_access import DB_access


class MemberActivityPastUtils:
    def __init__(self, db_access: DB_access) -> None:
        self.db_access = db_access

    def update_joined_accounts(
        self,
        start_dt: datetime,
        end_dt: datetime,
        all_joined_day: dict[str, set[str]],
        starting_key: int,
        window_d: int = 7,
    ):
        """
        Parameters:
        -----------
        start_dt : datetime
            the starting point of looking into joined accounts
        end_dt : datetime
            the ending point of looking into joined accounts
        all_joined_day : dict[str, set[str]]
            dictionary of `all_joined_day` from before
            we should update this dict based on the new joined accounts
            difference between this one and `all_joined` is
            `all_joined` is for past `window_d` days but `all_joined_day`
            is for users joining for just the day
        starting_key : int
            the starting key to add the joined accounts
        window_d : int
            the window days to include days
            default is 7 days


        Returns:
        ---------
        all_joined : dict[str, set[str]]
            the updated joined dictionary for past 7 days
        all_joined_day : dict[str, set[str]]
            the updated joined dictionary for past 1 day
        """
        # to get the data in end_date we should plus it to one
        joined_acc = self._get_joined_accounts(
            date_range=[start_dt, end_dt + timedelta(days=1)]
        )

        all_joined_day = self.update_all_joined_day(
            start_dt, end_dt, all_joined_day, starting_key, joined_acc
        )

        all_joined = self.get_users_past_days(all_joined_day, window_d)

        return all_joined, all_joined_day

    def get_users_past_days(
        self, all_joined_day: dict[str, set[str]], window_d: int
    ) -> dict[str, set[str]]:
        """
        get the users from past `window_d` days

        Parameters:
        --------------
        all_joined_day : dict[str, set[str]]
            the users joining in one day
        window_d : int
            the number of days look into past for getting users

        Returns:
        ----------
        all_joined : dict[str, list[str]]
            the users joining in, withing `window_d` past days
        """
        all_joined: dict[str, set[str]] = {}

        # looping up to the max key
        loop_max = array(list(all_joined_day.keys()), dtype=int).max() + 1

        for day_idx in range(loop_max):
            # how mAny days to look for past joined members
            look_past = None
            if day_idx - window_d > 0:
                look_past = day_idx - window_d
            else:
                look_past = 0

            joined_members_idx = array(range(look_past, day_idx + 1), dtype=str)
            all_joined[str(look_past + 1)] = set()
            for idx in joined_members_idx:
                all_joined[str(look_past + 1)] = all_joined[str(look_past + 1)].union(
                    all_joined_day[idx]
                )

        return all_joined

    def update_all_joined_day(
        self,
        start_dt: datetime,
        end_dt: datetime,
        all_joined_day: dict[str, set[str]],
        starting_key: int,
        joined_acc: list[dict[str, str]],
    ) -> dict[str, set[str]]:
        """
        update the all_joined_day dict with new retrieved data

        Parameters:
        -----------
        start_dt : datetime
            the starting point of looking into joined accounts
        end_dt : datetime
            the ending point of looking into joined accounts
        all_joined_day : dict[str, set[str]]
            dictionary of joined accounts from before
            we should update this dict based on the new joined accounts
        starting_key : int
            the starting key to add the joined accounts
        joined_acc : list[dict[str, str]]
            list of retrieved data from db
            it is a list of dicionaries, each has the keys of
             `joinedAt`, and `discordId`

        Returns:
        ---------
        all_joined_day : dict[str, set[str]]
            the updated joined dictionary
        """

        for i in range(0, (end_dt - start_dt).days + 1):
            date = (start_dt + timedelta(days=i)).date()
            joined_accounts = self._get_accounts_per_date(joined_acc, date)

            date_index = i + starting_key
            all_joined_day[str(date_index)] = set(joined_accounts)

        return all_joined_day

    def create_past_history_query(self, date_range: tuple[datetime, datetime]):
        """
        create a query to retreive the data that are not analyzed

        Parameters:
        -------------
        date_range: tuple[datetime, datetime]
            a list of length 2, the first index has the start of the interval
            and the second index is end of the interval

        Returns:
        ----------
        query : dictionary
            the query representing the dictionary of filters
        """
        date_interval_start = date_range[0]
        date_interval_end = date_range[1]

        query = {
            "date": {
                # the given date_range in script analysis
                "$gte": date_interval_start,
                "$lte": date_interval_end,
            }
        }

        return query

    def convert_back_to_old_schema(
        self,
        retrieved_data: list[dict],
        date_start: datetime,
        window_param: dict[str, str],
    ) -> dict:
        """
        convert the retrieved data back to the old schema we had, to do the analysis

        Parameters:
        ---------------
        retrieved_data : array
            array of db returned records
        date_start : datetime
            the starting point of analysis
        days_after_analysis_start : int
            the day count after analysis which are available in DB
        window_param : dict[str, str]
            the window parameters containing the step_size and period_size

        Returns:
        ----------
        activity_dict : dict
            the data converted to the old db schema
        """
        # make empty result dictionary
        activity_dict = {}

        # store results in dictionary
        activity_dict["all_joined"] = {}
        activity_dict["all_joined_day"] = {}
        activity_dict["all_consistent"] = {}
        activity_dict["all_vital"] = {}
        activity_dict["all_active"] = {}
        activity_dict["all_connected"] = {}
        activity_dict["all_paused"] = {}
        activity_dict["all_new_disengaged"] = {}
        activity_dict["all_disengaged"] = {}
        activity_dict["all_unpaused"] = {}
        activity_dict["all_returned"] = {}
        activity_dict["all_new_active"] = {}
        activity_dict["all_still_active"] = {}
        activity_dict["all_dropped"] = {}
        activity_dict["all_disengaged_were_vital"] = {}
        activity_dict["all_disengaged_were_newly_active"] = {}
        activity_dict["all_disengaged_were_consistently_active"] = {}
        activity_dict["all_lurker"] = {}
        activity_dict["all_about_to_disengage"] = {}
        activity_dict["all_disengaged_in_past"] = {}

        for idx in range(len(retrieved_data)):
            db_record = retrieved_data[idx]
            # parser.parse(db_record["date"]) - timedelta(
            #     days=window_param["period_size"]
            # )

            for activity in activity_dict.keys():
                try:
                    if db_record[activity] != []:
                        # creating a dictionary of users
                        users_name = db_record[activity]
                        # make a dictionary of indexes and users for
                        # a specific activity in an specific day
                        activity_dict[activity][str(idx)] = set(users_name)
                    else:
                        activity_dict[activity][str(idx)] = set("")
                except KeyError:
                    logging.error(
                        f"KeyError: the key {activity} is not available in DB record!"
                    )
                except Exception as exp:
                    logging.error(str(exp))

        activity_dict["first_end_date"] = (
            date_start - timedelta(days=window_param["period_size"])
        ).isoformat()

        return activity_dict

    def _get_accounts_per_date(
        self, joined_acc, date, date_key="joined_at", account_key="id"
    ):
        """
        get the accounts for a special date

        Parameters:
        -------------
        joined_acc : list(dict[str, Any])
            joined account retreived from database
            it must be sorted by joinDate!
        date : datetime.date
            the date that we're going to retrieve accounts from
        date_key : str
            the key used to represent the date of user join
        account_key : str
            the key used to represent the account name

        Returns:
        ---------
        account_names : list(str)
            the list of accounts
        """
        account_names = []

        for account in joined_acc:
            account_join_date = account[date_key].date()
            if account_join_date == date:
                account_names.append(account[account_key])

        return account_names

    def _get_joined_accounts(
        self, date_range: tuple[datetime, datetime]
    ) -> list[dict[str, Any]]:
        """
        get the joined accounts for a time interval to a date range

        Parameters:
        -------------
        date_range : tuple of datetime
            a tuple with length 2
            in the first index we save the starting date
            in the second date we would save the end date

        Returns:
        ----------
        data : list of dictionaries
            an array of dictionaries
            each dictionary has `account` and `joinDate` member
        """
        query = {"joined_at": {"$gte": date_range[0], "$lte": date_range[1]}}
        feature_projection = {"joined_at": 1, "id": 1, "_id": 0}

        # quering the db now
        cursor = self.db_access.query_db_find("rawmembers", query, feature_projection)

        data = list(cursor)

        return data

    def _append_all_past_data(
        self, retrived_past_data, activity_names_list, starting_idx=0
    ):
        """
        Append all past activities together

        Parameters:
        --------------
        retrived_past_data : list
            list of dictionaries, having all the activities in it
        activity_names_list : list
            the activities to filter
        starting_idx : int
            the data of activities that should be started from the index
            in another words it would be started from which day
            default is 0, meaning all the past data from the starting point of
            `first_end_date` will be included

        Returns:
        ----------
        all_activity_data_dict : dictionary
            the analyzed data with refined keys
        maximum_key : int
            the maximum key of the data
        """
        all_activity_data_dict = {}
        maximum_key_values = []
        for activity_name in activity_names_list:
            activity_data_list = retrived_past_data[activity_name]

            activity_data_dict, max_key_val = self._refine_dict_indexes(
                activity_data_list, starting_idx
            )

            maximum_key_values.append(max_key_val)
            # add it to the new dictionary
            all_activity_data_dict[activity_name] = activity_data_dict

        return all_activity_data_dict, max(maximum_key_values)

    def _refine_dict_indexes(self, data_dict, starting_idx=0):
        """
        refine the indexes in dictionary

        Parameters:
        ------------
        data_dict : dictionary
            dictionary for a specific activity with keys '0','1', '2', etc
        starting_idx : int
            the data of activities that should be started from the index
            in another words it would be started from which day
            default is 0, meaning all the past data from the starting point of
            `first_end_date` will be included

        Returns:
        -----------
        data_dict_appended: dictionary
            all the dictionaries appended together
            the keys are refined in a way that
             starting with '0' and ending with sum of keys
        max_key_val : int
            the maximum value of the dictionary
        """
        data_dict_appended = {}
        max_key_val = 0

        # get all the keys in integer format
        indices_list = list(map(lambda x: int(x), data_dict.keys()))
        # converting to numpy to be able to filter them
        indices_list = array(indices_list)
        # filtering them
        indices_list = indices_list[indices_list > starting_idx]
        # incrementing and converting the indices of the dictionary to string
        indices_list = list(map(lambda x: str(x + max_key_val), indices_list))
        # creating new dictionary with new indices
        dictionary_refined_keys = dict(zip(indices_list, list(data_dict.values())))
        # adding it to the results dictionary
        data_dict_appended.update(dictionary_refined_keys)

        # if there was some index available
        if len(indices_list) != 0:
            max_key_val += int(max(indices_list))

        return data_dict_appended, max_key_val
