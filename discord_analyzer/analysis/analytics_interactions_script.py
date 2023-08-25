import itertools
from datetime import datetime
from warnings import warn

from numpy import zeros


def sum_interactions_features(cursor_list, dict_keys):
    """
    sum the interactions per hour
    Parameters:
    ------------
    cursor_list : list
       the db cursor returned and converted as list
    dict_keys : list
       the list of dictionary keys, representing the features in database

    Returns:
    ----------
    summed_counts_per_hour : dictionary
       the dictionary of each feature having summed
        the counts per hour, the dictionary of features is returned
    """

    summed_counts_per_hour = {}
    for key in dict_keys:
        summed_counts_per_hour[key] = zeros(24)

    for key in dict_keys:
        # the array of hours 0:23
        for data in cursor_list:
            summed_counts_per_hour[key] += data[key]

    return summed_counts_per_hour


def per_account_interactions(
    cursor_list,
    dict_keys=["replier_accounts", "reacter_accounts", "mentioner_accounts"],
):
    """
    get per account interactions as `mentioner_accounts`,
    `reacter_accounts`, and `replier_accounts` (summing)
    Parameters:
    ------------
    cursor_list : list
        the db cursor returned and converted as list
    dict_keys : list
        the list of dictionary keys, representing the features in database

    Returns:
    ----------
    summed_per_account_interactions : dictionary
        the dictionary of each feature having summed the counts per hour,
          the dictionary of features is returned
    """

    data_processed = {}
    all_interaction_accounts = {}

    # for each interaction
    for k in dict_keys:
        temp_dict = {}
        # get the data of a key in a map
        samples = list(map(lambda data_dict: data_dict[k], cursor_list))

        # flatten the list
        samples_flattened = list(itertools.chain(*samples))

        for i, sample in enumerate(samples_flattened):
            account_name = sample[0]["account"]
            interaction_count = sample[0]["count"]

            if account_name not in temp_dict.keys():
                temp_dict[account_name] = interaction_count
            else:
                temp_dict[account_name] += interaction_count

            if account_name not in all_interaction_accounts.keys():
                all_interaction_accounts[account_name] = interaction_count
            else:
                all_interaction_accounts[account_name] += interaction_count

        data_processed[k] = refine_dictionary(temp_dict)

    data_processed["all_interaction_accounts"] = refine_dictionary(
        all_interaction_accounts
    )

    summed_per_account_interactions = data_processed

    return summed_per_account_interactions


def refine_dictionary(interaction_dict):
    """
    refine dictionary and add the account id to the dictionary

    Parameters:
    ------------
    interaction_dict : dict
        a dictionary like {'user1': 5, 'user2: 4}
        keys are usernames and values are the count of each user interaction

    Returns:
    ----------
    refined_dict : nested dictionary
        the input refined like this
        {
            '0': { 'user1': 5 },
            '1': { 'user2': 4 }
        }
    """

    refined_dict = {}
    for idx, data_acc in enumerate(interaction_dict.keys()):
        refined_dict[f"{idx}"] = {
            "account": data_acc,
            "count": interaction_dict[data_acc],
        }

    return refined_dict


def filter_channel_name_id(
    cursor_list, channel_name_key="channelName", channel_id_key="channelId"
):
    """
    filter the cursor list retrieved from DB for channels and their ids

    Parameters:
    -------------
    cursor_list : list of dictionaries
        the retreived values of DB
    channel_name_key : string
        the name of channel_name field in DB
        default is `channel`
    channel_id_key : string
        the name of channel_id field in DB
        default is `channelId`
    Returns:
    ----------
    channels_id_dict : dictionary
        a dictionary with keys as channel_id and values as channel_name
    """
    channels_id_dict = {}
    for ch_id_dict in cursor_list:
        # the keys in dict are channel id
        chId = ch_id_dict[channel_id_key]
        # and the values of dict are the channel name
        channels_id_dict[chId] = ch_id_dict[channel_name_key]

    return channels_id_dict


def filter_channel_thread(
    cursor_list,
    # channels_id,
    # channels_id_name,
    thread_name_key="threadName",
    author_key="author",
    message_content_key="content",
    date_key="createdDate",
):
    """
    create a dictionary of channels and threads for messages,
      sorted by time ascending

      Note: The cursor_list `MUST` be sorted ascending.

    Parameters:
    ------------
    cursor_list : list of dictionaries
        the list of values in DB containing a thread and messages of authors
    # channels_id : list
    #     a list of channels id
    #     minimum length of the list is 1
    # channels_id_name : dict
    #     the dictionary containing {`channelId`: `channel_name`}
    thread_name_key : string
        the name of the thread field in DB
    author_key : string
        the name of the author field in DB
    message_content_key : string
        the name of the message content field in DB
    date_key : str
        the key to check whether the data is descending or not

    Returns:
    ----------
    channel_thread_dict : {str:{str:{str:str}}}
        a dictionary having keys of channel names,
            and per thread messages as dictionary
    # An example of output can be like this:
    {
        “CHANNEL_NAME1” :
        {
            “THREAD_NAME1” :
            {
                “1:@user1”: “Example message 1”,
                “2:@user2”: “Example message 2”,
                …
            },
            “THREAD_NAME2” :
                {More example messages in same format}, …},
        “CHANNEL_NAME2” :
            {More thread dictionaries with example messages in same format}, …},
        More channel dictionaries with thread dictionaries
            with example messages in same format,
            …
    }
    """
    # check the input is descending
    date_check = datetime(1961, 1, 1)
    for data in cursor_list:
        msg_date = datetime.strptime(data[date_key], "%Y-%m-%d %H:%M:%S")
        if msg_date >= date_check:
            date_check = msg_date
            continue
        else:
            warn("Messages is not ascending ordered!")

    # First we're filtering the records via their channel name
    channels_dict = {}
    # create an empty array of each channel
    # for chId in channels_id:
    for record in cursor_list:
        ch = record["channelName"]
        if ch not in channels_dict:
            channels_dict[ch] = [record]
        else:
            channels_dict[ch].append(record)

    # filtering through the channel name field in dictionary
    # for record in cursor_list:
    #     # chId = record["channelId"]
    #     # ch = channels_id_name[chId]
    #     channels_dict[ch].append(record)

    # and the adding the filtering of thread id
    channel_thread_dict = {}

    # filtering threads
    for ch in channels_dict.keys():
        channel_thread_dict[ch] = {}
        # initialize the index
        idx = 1
        for record in channels_dict[ch]:
            # get the thread name
            thread = record[thread_name_key]

            # if the thread wasn't available in dict
            # then make a dictionary for that
            if thread not in channel_thread_dict[ch].keys():
                # reset the idx for each thread
                idx = 1
                # creating the first message
                channel_thread_dict[ch][thread] = {
                    f"{idx}:{record[author_key]}": record[message_content_key]
                }

            # if the thread was created before
            # then add the author content data to the dictionary
            else:
                # increase the index for the next messages in thread
                idx += 1
                channel_thread_dict[ch][thread][f"{idx}:{record[author_key]}"] = record[
                    message_content_key
                ]

    return channel_thread_dict
