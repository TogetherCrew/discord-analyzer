# checking the past history of member activities

from datetime import datetime, timedelta

from tc_analyzer_lib.algorithms.utils.member_activity_history_utils import (
    MemberActivityPastUtils,
)
from tc_analyzer_lib.DB_operations.mongodb_access import DB_access


# the main script function
def check_past_history(
    db_access: DB_access,
    date_range: list[datetime],
    window_param: dict[str, int],
    collection_name: str = "memberactivities",
    verbose=False,
):
    """
    check past member_activities history and
    return if some analysis were available in db in the date_range

    Parameters:
    -------------
    db_access: DB_access
        the database access class that queries are called through it
    date_range: tuple[datetime, datetime]
        a tiple of length 2, the first index has the start of the interval
        and the second index is end of the interval
    window_param : dict[str, int]
        a dictionary 2 values, the keys and values
        - "period_size": window size in days. default = 7
        - "step_size": step size of sliding window in days. default = 1
    collection_name: string
        the collection of db to use
        default is `memberactivities`
    verbose : bool
        whether to print the logs or not

    Returns:
    ----------
    all_activity_data_dict : dictionary
        the data for past activities
    new_date_range : tuple
        tuple of new date range in datetime format
        because the last
    maximum_key : int
        the maximum key that the new data should start its data from
    """
    # checking the inputs
    if len(date_range) != 2:
        raise ValueError(
            f"""date_range should have the length of two,
          first index is the start of the interval and the
          second index is the end of the interval
          its length is: {len(date_range)}"""
        )

    date_range_start = date_range[0]
    date_range_end = date_range[1]

    member_act_past_utils = MemberActivityPastUtils(db_access=db_access)

    # creating the query
    query = member_act_past_utils.create_past_history_query(date_range)

    # do not project the variables that we don't need
    feature_projection = {
        # 'first_end_date': 1,
        # 'all_consistent': 1,
        "_id": 0
    }
    # sorting the results from past to now (ascending)
    # sorting by `date`
    sorting = ["date", 1]

    # quering the db now
    cursor = db_access.query_db_find(
        collection_name, query, feature_projection, sorting
    )
    # getting a list of returned data
    past_data_new_schema = list(cursor)

    db_analysis_end_date: datetime | None

    # if any past data was available in DB
    if past_data_new_schema != []:
        if verbose:
            print(past_data_new_schema)

        # db_analysis_start_date = parser.parse(past_data[0]['date'])
        # db_analysis_start_date = date_range_start
        db_analysis_end_date = past_data_new_schema[-1]["date"]

        # days_after_analysis_start = (
        #       db_analysis_end_date - db_analysis_start_date
        # ).days

        past_data = member_act_past_utils.convert_back_to_old_schema(
            past_data_new_schema,
            date_range_start,
            window_param=window_param,
        )

    else:
        # db_analysis_start_date = None
        db_analysis_end_date = None

    # # the input date_range in format of datetime
    # # converting the dates into datetime format
    # date_format = "%y/%m/%d"
    # date_range_start = datetime.datetime.strptime(date_range[0], date_format)
    # date_range_end = datetime.datetime.strptime(date_range[1], date_format)

    new_date_range: list[datetime]
    # if for the requested date_range, its results were available in db
    if (db_analysis_end_date is not None) and (date_range_start < db_analysis_end_date):
        # refine the dates
        # if the range end was smaller than the analysis end,
        #  then empty the new_date_range
        # empty it, since all the requested analysis are available in db
        if date_range_end <= db_analysis_end_date:
            new_date_range = []
        else:
            # start date would be the next day of the end day
            new_date_range = [
                db_analysis_end_date + timedelta(days=1),
                date_range_end,
            ]

        all_activity_data_dict = past_data
        # maximum key is used for having the key for future data
        # maximum_key = days_after_analysis_start + 1
        maximum_key = len(past_data_new_schema)
    else:
        all_activity_data_dict = {}
        new_date_range = [date_range_start, date_range_end]
        maximum_key = 0

    return all_activity_data_dict, new_date_range, maximum_key
