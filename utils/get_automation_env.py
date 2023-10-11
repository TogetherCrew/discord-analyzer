import os

from dotenv import load_dotenv


def get_automations_env() -> dict[str, str]:
    """
    get the automations env variables

    Returns
    ---------
    env_vars : dict[str, str]
        the environment variables for automation service
        the keys would be `DB_NAME`, and `COLLECTION_NAME`
        and values are right values
    """
    load_dotenv()
    db_name = os.getenv("AUTOMATION_DB_NAME", "")
    collection_name = os.getenv("AUTOMATION_DB_COLLECTION", "")

    return {"DB_NAME": db_name, "COLLECTION_NAME": collection_name}
