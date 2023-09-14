from engagement_notifier.engagement import EngagementNotifier

from .utils.analyzer_setup import launch_db_access


def test_engagement_notifier_saga_creation():
    """
    test if the saga is created in the database
    """
    guildId = "1234"
    db_access = launch_db_access(guildId)
    db_access.db_mongo_client["Saga"].drop_collection("sagas")
    db_access.db_mongo_client["Saga"]["sagas"].delete_many({})

    notifier = EngagementNotifier()

    data = {
        "guildId": guildId,
        "created": False,
        "discordId": "user1",
        "message": "This message is sent you for notifications!",
        "userFallback": True,
    }
    saga_id = notifier._create_manual_saga(data=data)

    manual_saga = db_access.db_mongo_client["Saga"]["sagas"].find_one(
        {"sagaId": saga_id}
    )
    print(manual_saga)
    assert manual_saga["choreography"]["name"] == "DISCORD_NOTIFY_USERS"
    assert manual_saga["choreography"]["transactions"] == [
        {
            "queue": "DISCORD_BOT",
            "event": "SEND_MESSAGE",
            "order": 1,
            "status": "NOT_STARTED",
        }
    ]
    assert manual_saga["status"] == "IN_PROGRESS"
    assert manual_saga["data"] == data
    assert manual_saga["sagaId"] == saga_id
