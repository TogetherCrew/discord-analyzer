from tc_analyzer_lib.utils.get_automation_env import get_automations_env
from tc_analyzer_lib.utils.mongo import MongoSingleton

from .interfaces import Automation


class AutomationDB:
    def __init__(self):
        """
        the automation db instance
        """
        instance = MongoSingleton.get_instance()
        self.client = instance.get_client()
        at_env_vars = get_automations_env()
        self.db_name = at_env_vars["DB_NAME"]
        self.collection_name = at_env_vars["COLLECTION_NAME"]

    def load_from_db(self, guild_id: str) -> list[Automation]:
        """
        load all the automation from database for a guild based on guild_id

        Returns
        ---------
        guild_automations : list[Automation]
            a list of automation related for one guild
        """

        cursor = self.client[self.db_name][self.collection_name].find(
            {"guildId": guild_id}
        )

        automations = list(cursor)

        guild_automations = []
        for at in automations:
            guild_at = Automation.from_dict(at)
            guild_automations.append(guild_at)

        return guild_automations

    def save_to_db(self, automation: Automation | dict) -> None:
        """
        save one automation into the database

        Parameters
        ------------
        automation : Automation | dict
            an automation to insert into a guild
            in case of Automation instance,
            at first we would convert it to a dictionary
        """
        if isinstance(automation, dict):
            self.client[self.db_name][self.collection_name].insert_one(automation)
        elif isinstance(automation, Automation):
            self.client[self.db_name][self.collection_name].insert_one(
                automation.to_dict()
            )
        else:
            msg = "Not supported the type of entered object!,"
            msg += f"given type is: {type(automation)}"
            raise TypeError(msg)
