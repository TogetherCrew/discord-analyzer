#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  activity_hourly.py
#
#  Author Ene SS Rawa / Tjitse van der Molen


# # # # # import libraries # # # # #

import json

import numpy as np


def parse_reaction(s):
    result = []
    for subitem in s:
        items = subitem.split(",")
        parsed_items = []
        for item in items:
            parsed_items.append(item)
        result.append(parsed_items)
    return result


# # # # # main function # # # # #


def activity_hourly(
    json_file, out_file_name=None, acc_names=[], mess_substring=None, emoji_types=None
):
    """
    Counts activity per hour from json_file and stores in out_file_name

    Input:
    json_file - [JSON]: list of JSON objects with message data
    out_file_name - str: path and filename where output is stored
    acc_names - [str]: account names for which activity should be
        counted separately (default = [])
    mess_substring - [str]: only messages containing at least one
        substring in this list are considered. all messages are
        considered if set to None (default = None)
    emoji_types - [str]: only emojis in this list are considered. all
        emojis are considered if set to None (default = None)

    Output:
    warning_count - [int]: list of counts for the different possible
        warnings that could be raised by the script:
        1st entry: number of messages sent by an author not listed in
            acc_names
        2nd entry: number of times that a duplicate DayActivity object
            is encounterd. if this happens, the first object in the list
            is used.
        3rd entry: number of times a message author mentions themselves
            in the message. these mentions are not counted
        4rd entry: number of times a message author emoji reacts to
            their own message. these reactions are not counted
        5th entry: number of times an emoji sender is not in acc_names
        6th entry: number of times a mentioned account is not in
            acc_names
        7th entry: number of times an account that is replied to is not
            in acc_names

    Notes:
    The results are saved as JSON objects based on out_file_name
    """

    # initiate array with zeros for counting error occurences
    warning_count = [0] * 7

    # initiate empty result array for DayActivity objects all_day_activity_obj = []

    # add remainder category to acc_names
    acc_names.append("remainder")
    all_day_activity_obj = []
    # for each message
    for mess in json_file:
        # # # check for specific message content # # #

        # if message contains specified substring (or None are specified)
        if (mess_substring is None) or (
            any([ss in mess["message_content"] for ss in mess_substring])
        ):
            # # # extract data # # #

            # obtain message date, channel and author and reply author
            mess_date = mess["datetime"].strftime("%Y-%m-%d")
            mess_hour = int(mess["datetime"].strftime("%H"))
            mess_chan = mess["channel"]
            mess_auth = mess["author"]
            rep_auth = mess["replied_user"]

            reactions = parse_reaction(mess["reactions"])

            try:
                # obtain index of author in acc_names
                auth_i = acc_names.index(mess_auth)
            except Exception as exp:
                # if author is not in acc_names,
                # raise warning and add counts to remainder
                print(
                    f"WARNING: author name {mess_auth} not found in acc_names",
                    f"Exception: {exp}",
                )
                warning_count[0] += 1
                auth_i = -1

            if rep_auth is not None:
                try:
                    # obtain index of reply author in acc_names
                    rep_i = acc_names.index(rep_auth)
                except Exception as exp:
                    # if author is not in acc_names, raise warning
                    #  and add counts to remainder
                    print(
                        f"WARNING: author name {rep_auth} not found in acc_names",
                        f"Exception: {exp}",
                    )
                    warning_count[6] += 1
                    rep_i = -1
            else:
                rep_i = None

            # # # obtain object index in object list # # #

            # see if an object exists with corresponding date and channel
            (all_day_activity_obj, obj_list_i, warning_count) = get_obj_list_i(
                all_day_activity_obj, mess_date, mess_chan, acc_names, warning_count
            )

            # # # count activity per hour # # #

            # count reactions
            (n_reac, reacting_accs, warning_count) = count_reactions(
                reactions, emoji_types, mess_auth, warning_count
            )

            # if there are any reacting accounts
            if len(reacting_accs) > 0:
                # for each reacting account
                for r_a in reacting_accs:
                    # add reacting accounts
                    all_day_activity_obj[obj_list_i].reacted_per_acc[auth_i].append(r_a)

            # add n_reac to hour of message that received the emoji
            all_day_activity_obj[obj_list_i].reacted[auth_i, mess_hour] += int(n_reac)

            # count raised warnings
            warning_count[4] += count_from_list(
                reacting_accs,
                acc_names,
                all_day_activity_obj[obj_list_i].reacter,
                mess_hour,
            )

            # count mentions
            (n_men, n_rep_men, mentioned_accs, warning_count) = count_mentions(
                mess["user_mentions"], rep_auth, mess_auth, warning_count
            )

            # if there are any mentioned accounts
            if len(mentioned_accs) > 0:
                # for each mentioned account
                for m_a in mentioned_accs:
                    # add mentioned accounts
                    all_day_activity_obj[obj_list_i].mentioner_per_acc[auth_i].append(
                        m_a
                    )

            # if message was not sent in thread
            if mess["threadId"] is None:
                # if message is default message
                if mess["mess_type"] == 0:
                    # add 1 to hour of message
                    all_day_activity_obj[obj_list_i].lone_messages[
                        auth_i, mess_hour
                    ] += int(1)

                    # add n_men to hour for message sender
                    all_day_activity_obj[obj_list_i].mentioner[
                        auth_i, mess_hour
                    ] += int(n_men)

                    # count raised warnings
                    warning_count[5] += count_from_list(
                        mentioned_accs,
                        acc_names,
                        all_day_activity_obj[obj_list_i].mentioned,
                        mess_hour,
                    )

                # if message is reply
                elif mess["mess_type"] == 19:
                    # store account name that replied
                    # for author of message that was replied to
                    all_day_activity_obj[obj_list_i].replied_per_acc[rep_i].append(
                        mess_auth
                    )

                    # add 1 to hour of message for replier
                    all_day_activity_obj[obj_list_i].replier[auth_i, mess_hour] += 1

                    # add 1 to hour of message for replied
                    all_day_activity_obj[obj_list_i].replied[rep_i, mess_hour] += 1

                    # add n_men to hour for message sender
                    all_day_activity_obj[obj_list_i].mentioner[
                        auth_i, mess_hour
                    ] += int(n_men)

                    # count raised warnings
                    warning_count[5] += count_from_list(
                        mentioned_accs,
                        acc_names,
                        all_day_activity_obj[obj_list_i].mentioned,
                        mess_hour,
                    )

                    # add n_rep_men to hour of message
                    all_day_activity_obj[obj_list_i].rep_mentioner[
                        auth_i, mess_hour
                    ] += int(n_rep_men)
                    all_day_activity_obj[obj_list_i].rep_mentioned[
                        rep_i, mess_hour
                    ] += int(n_rep_men)

                    # if reply is to unknown account
                    # and this account got mentioned in the reply
                    if n_rep_men > 0 and rep_i == -1:
                        print(
                            "WARNING: acc name {} not found in acc_names".format(
                                rep_auth
                            )
                        )
                        warning_count[5] += 1

            # if message was sent in thread
            else:
                # if message is default message
                if mess["mess_type"] == 0:
                    # add 1 to hour of message
                    all_day_activity_obj[obj_list_i].thr_messages[
                        auth_i, mess_hour
                    ] += int(1)
                    # add n_men to hour for message sender
                    all_day_activity_obj[obj_list_i].mentioner[
                        auth_i, mess_hour
                    ] += int(n_men)

                    # count raised warnings
                    warning_count[5] += count_from_list(
                        mentioned_accs,
                        acc_names,
                        all_day_activity_obj[obj_list_i].mentioned,
                        mess_hour,
                    )
                # if message is reply
                elif mess["mess_type"] == 19:
                    # store account name that replied
                    # for author of message that was replied to
                    all_day_activity_obj[obj_list_i].replied_per_acc[rep_i].append(
                        mess_auth
                    )

                    # add 1 to hour of message for replier
                    all_day_activity_obj[obj_list_i].replier[auth_i, mess_hour] += 1

                    # add 1 to hour of message for replied
                    all_day_activity_obj[obj_list_i].replied[rep_i, mess_hour] += int(1)

                    # add n_men to hour for message sender
                    all_day_activity_obj[obj_list_i].mentioner[
                        auth_i, mess_hour
                    ] += int(n_men)

                    # count raised warnings
                    warning_count[5] += count_from_list(
                        mentioned_accs,
                        acc_names,
                        all_day_activity_obj[obj_list_i].mentioned,
                        mess_hour,
                    )

                    # add n_rep_men to hour of message
                    all_day_activity_obj[obj_list_i].rep_mentioner[
                        auth_i, mess_hour
                    ] += int(n_rep_men)
                    all_day_activity_obj[obj_list_i].rep_mentioned[
                        rep_i, mess_hour
                    ] += int(n_rep_men)

                    # if reply is to unknown account
                    # and this account got mentioned in the reply
                    if n_rep_men > 0 and rep_i == -1:
                        print(
                            "WARNING: acc name {} not found in acc_names".format(
                                rep_auth
                            )
                        )
                        warning_count[5] += 1

    # # # store results    # # #
    # json_out_file = store_results_json([i.asdict() for i in \
    #     all_day_activity_obj], out_file_name)
    return (warning_count, [i.asdict() for i in all_day_activity_obj])


# # # # # classes # # # # #


class DayActivity:
    # define constructor
    def __init__(
        self,
        date,
        channel,
        lone_messages,
        thr_messages,
        replier,
        replied,
        mentioner,
        mentioned,
        rep_mentioner,
        rep_mentioned,
        reacter,
        reacted,
        reacted_per_acc,
        mentioner_per_acc,
        replied_per_acc,
        acc_names,
    ):
        self.date = date  # date of object
        self.channel = channel  # channel id of object
        # number of lone messages per hour per account
        self.lone_messages = lone_messages
        # number of thread messages per hour per account
        self.thr_messages = thr_messages
        self.replier = replier  # number of replies sent per hour per account
        # number of replies received per hour per account
        self.replied = replied
        self.mentioner = mentioner  # number of mentions sent per hour per account
        # number of mentions received per hour per account
        self.mentioned = mentioned
        # number of reply mentions sent per hour per account
        self.rep_mentioner = rep_mentioner
        # number of reply mentions received per hour per account
        self.rep_mentioned = rep_mentioned
        # number of reactions sent per hour per account
        self.reacter = reacter
        # number of reactions received per hour per account
        self.reacted = reacted
        # list of account names from which reactions
        # are received per account (duplicates = multiple reactions)
        self.reacted_per_acc = reacted_per_acc
        # list of account names that are mentioned by
        # account per account (duplicates = multiple mentions)
        self.mentioner_per_acc = mentioner_per_acc
        # list of account names from which replies are
        # received per account (duplicates = multiple replies)
        self.replied_per_acc = replied_per_acc
        # account names (corresponds to row index of activity types)
        self.acc_names = acc_names

    # # # functions # # #

    # turn object into dictionary

    def asdict(self):
        return {
            "date": self.date,
            "channel": self.channel,
            "lone_messages": self.lone_messages.tolist(),
            "thr_messages": self.thr_messages.tolist(),
            "replier": self.replier.tolist(),
            "replied": self.replied.tolist(),
            "mentioner": self.mentioner.tolist(),
            "mentioned": self.mentioned.tolist(),
            "rep_mentioner": self.rep_mentioner.tolist(),
            "rep_mentioned": self.rep_mentioned.tolist(),
            "reacter": self.reacter.tolist(),
            "reacted": self.reacted.tolist(),
            "reacted_per_acc": self.reacted_per_acc,
            "mentioner_per_acc": self.mentioner_per_acc,
            "replied_per_acc": self.replied_per_acc,
            "acc_names": self.acc_names,
        }


# # # # # functions # # # # #


def get_obj_list_i(
    all_day_activity_obj, mess_date, mess_chan, acc_names, warning_count
):
    """
    Assesses index of DayActivity object

    Input:
    all_day_activity_obj - [obj]: list of DayActivity objects
    mess_date - str: date in which message was sent yyyy-mm-dd
    mess_chan - str: name of channel in which message was sent
    num_rows - int: number of rows for count arrays in DayActivity

    Output:
    all_day_activity_obj - [obj]: updated list of DayActivity objects
    obj_list_i - int: index of DayActivity object in
        all_day_activity_obj that corresponds to the message

    Notes:
    if no corresponding DayActivity object is found in
    all_day_activity_obj, a new DayActivity object is appended
    """

    # check if DayActivity object corresponding to mess_date and mess_chan exists
    obj_overlap = [
        all(
            [
                getattr(obj, "date", "Attribute does not exist")[0] == mess_date,
                getattr(obj, "channel", "Attribute does not exist")[0] == mess_chan,
            ]
        )
        for obj in all_day_activity_obj
    ]

    # if there is no object for the channel date combination
    if not any(obj_overlap):
        # create DayActivity object and add it to the list
        all_day_activity_obj.append(
            DayActivity(
                [mess_date],
                [mess_chan],
                np.zeros((len(acc_names), 24), dtype=np.int16),
                np.zeros((len(acc_names), 24), dtype=np.int16),
                np.zeros((len(acc_names), 24), dtype=np.int16),
                np.zeros((len(acc_names), 24), dtype=np.int16),
                np.zeros((len(acc_names), 24), dtype=np.int16),
                np.zeros((len(acc_names), 24), dtype=np.int16),
                np.zeros((len(acc_names), 24), dtype=np.int16),
                np.zeros((len(acc_names), 24), dtype=np.int16),
                np.zeros((len(acc_names), 24), dtype=np.int16),
                np.zeros((len(acc_names), 24), dtype=np.int16),
                [[] for _ in range(len(acc_names))],
                [[] for _ in range(len(acc_names))],
                [[] for _ in range(len(acc_names))],
                acc_names,
            )
        )

        # set list index for message
        # TODO: Why it was -1?
        obj_list_i = int(-1)

    else:
        # set list index for message
        obj_list_i = int(obj_overlap.index(True))

        # see if object only occurs once and raise error if more than once
        if sum(obj_overlap) > 1:
            msg = "WARNING: duplicate DayActivity "
            msg += "object, first entry in list is used"
            print(msg)
            warning_count[1] += 1

    return all_day_activity_obj, obj_list_i, warning_count


# # #


def count_mentions(mess_mentions, replied_user, mess_auth, warning_count):
    """
    Counts number of user mentions in a message

    Input:
    mess_mentions - [str]: all user account names that are mentioned in
        the message
    replied_user - str: account name of author who is replied to if
        message type is reply
    mess_auth - str: message author

    Output:
    n_men - int: number of mentions in message
    n_rep_men - int: number of times the author of the message that is
        replied to is mentioned in the message
    reacting_accs - [str]: all account names that were mentioned

    Notes:
    authors mentioning themselves are not counted
    """

    # set number of interactions to 0
    n_men = 0
    n_rep_men = 0
    mentioned_accs = []

    # for each mentioned account
    for mentioned in mess_mentions:
        if mentioned is not None and len(mentioned) > 0:
            # if mentioned account is the same as message author
            if mentioned == mess_auth:
                # print error and skip
                msg = f"WARNING: {mess_auth} mentioned themselves. "
                msg += "This is not counted"
                print(msg)
                warning_count[2] += 1

            else:
                # if mentioned account is not the account that was replied to
                if mentioned != replied_user:
                    # add 1 to number of mentions
                    n_men += 1

                    # add mentioned account to mentioned_accs
                    mentioned_accs.append(mentioned)

                else:
                    # add 1 to number of replied mentions
                    n_rep_men = 1

    return n_men, n_rep_men, mentioned_accs, warning_count


# # #


def count_reactions(mess_reactions, emoji_types, mess_auth, warning_count):
    """
    Counts number of reactions to a message

    Input:
    mess_reactions - [[str]]: list with a list for each emoji type,
        containing the accounts that reacted with this emoji and the
        emoji type (last entry of lists within list)
    emoji_types - [str] or None: list of emoji types to be considered.
        All emojis are considered when None
    mess_auth - str: message author
    warning_count - [int]: list with counts for warning types

    Output:
    n_reac - int: number of emoji reactions to post
    reacting_accs - [str]: all account names that sent an emoji (if
        account sent >1 emoji, account name will be listed >1)
    warning_count - [int]: upated list with counts for warning types

    notes:
    emojis reacted by the author of the message are not counted but lead
        to a warning instead
    """
    # set number of reactions to 0
    n_reac = 0

    # make empty list for all accounts that sent an emoji
    reacting_accs = []

    # for every emoji type
    for emoji_type in mess_reactions:
        # if reacting account is in acc_names and
        # reacted emoji is part of emoji_types if defined
        if emoji_types is None or emoji_type[-1] in emoji_types:
            # for each account that reacted with this emoji
            for reactor in emoji_type[:-1]:
                # if the message author posted the emoji
                if reactor == mess_auth:
                    # print error and skip
                    msg = f"WARNING: {mess_auth} reacted to themselves."
                    msg += " This is not counted"
                    print(msg)
                    warning_count[3] += 1

                # if the reactor is not empty
                elif len(reactor) > 0:
                    # add 1 to number of reactions
                    n_reac += 1

                    # store name of reactor
                    reacting_accs.append(reactor)

    return n_reac, reacting_accs, warning_count


# # #


def count_from_list(acc_list, acc_names, to_count, mess_hour):
    """
    Adds counts per hour to accounts from list

    Input:
    acc_list - [str]: all account names that should be counted (the
        account is counted for each time it is in the list, allowing for
        duplicates)
    acc_names - [str]: account names for which activity should be
        counted separately
    to_count - [[int]]: activity type to be counted
    mess_hour - int: hour at which message with activity was sent

    Output:
    warning_count - int: number of times warning was raised

    Notes:
    counts are added to DayActivity object under the to_count variable
    """

    # initiate warning count at 0
    warning_count = 0

    # for each account
    for acc in acc_list:
        try:
            # obtain index of account name in acc_names
            acc_i = acc_names.index(acc)

        except Exception as exp:
            # if acc is not in acc_names, raise warning and add count to remainder
            msg = f"WARNING: acc name {acc} not found in acc_names"
            msg += f", Exception: {exp}"
            print(msg)
            warning_count += 1
            acc_i = -1

        # add 1 to hour of message for acc
        to_count[acc_i, mess_hour] += int(1)

    return warning_count


# # #


def store_results_json(save_dict, file_name, print_out=False):
    """
    Stores dictionary or list of dictionaries as JSON file

    Input:
    save_dict - {}, [{}]: dictionary or list of dictionaries to be saved
    file_name - str: name (including path) to where data is saved
    print_out - bool: whether message should be printed confirming that
        the data is saved

    Output:
    out_file - JSON: JSON file with content from save_dict

    Notes:
    JSON file is also saved in location specified by file_name
    """

    # initiate output file
    with open(file_name, "w") as f:
        # store results
        json.dump(save_dict, f)

    # # save and close output file
    # out_file.close()

    if print_out:
        print("data saved at: " + file_name)
