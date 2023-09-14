# flake8: noqa
# predefined messages are hardcoded for now here


def get_disengaged_message(name: str) -> str:
    """
    get a predefined message that we want to send to disengaged members

    Parameters
    ------------
    name : str
        the name to use in the message

    Returns
    --------
    disengaeged_message : str
        the message to send to the user
    """
    disengaeged_message = "## Hey {} :wave:\n".format(name)
    disengaeged_message += "We noticed you stopped engaging in RnDAO. Please know you're welcome to return when it works for you, and we're here to help!\n"
    disengaeged_message += "### If you have any feedback\n"
    disengaeged_message += "(even if it's just to say where you're dedicating your time these days), please share it with us, we'd love to learn :slight_smile: <https://forms.gle/XsLEXNGtYg3FZmnF8>\n"
    disengaeged_message += "### If you were looking to contribute\n"
    disengaeged_message += "- Join our next onboarding call (Wednesdays at 3 pm UTC https://discord.com/channels/915914985140531240/941648148068171816)\n"
    disengaeged_message += "- Or have a call with Rezvan, our community supporter: <https://meetwithwallet.xyz/rezvan>\n"
    disengaeged_message += "### If you were looking to learn \n"
    disengaeged_message += (
        "For new techniques and tools for human collaboration, checkout\n"
    )
    disengaeged_message += "- Learning channel https://discord.com/channels/915914985140531240/920707473369878589 for discussion and content\n"
    disengaeged_message += "- Our research blog: <https://rndao.mirror.xyz/>\n"
    disengaeged_message += "- Recorded talks: <https://www.youtube.com/@rndaotalks>\n"

    return disengaeged_message
