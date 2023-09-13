from datetime import datetime, timedelta

from engagement_notifier.engagement import EngagementNotifier

from .utils.analyzer_setup import launch_db_access


def test_engagement_notifier_fire_message_check_rabbitmq():
    """
    check the fired message count in rabbitmq queue
    """
    guildId = "1234"
    db_access = launch_db_access(guildId)

    db_access.db_mongo_client[guildId].drop_collection("memberactivities")
    db_access.db_mongo_client[guildId]["memberactivities"].delete_many({})

    db_access.db_mongo_client["Saga"].drop_collection("saga")

    date_yesterday = (
        (datetime.now() - timedelta(days=1))
        .replace(hour=0, minute=0, second=0)
        .strftime("%Y-%m-%dT%H:%M:%S")
    )

    date_two_past_days = (
        (datetime.now() - timedelta(days=2))
        .replace(hour=0, minute=0, second=0)
        .strftime("%Y-%m-%dT%H:%M:%S")
    )

    db_access.db_mongo_client[guildId]["memberactivities"].insert_many(
        [
            {
                "date": date_yesterday,
                "all_joined": [],
                "all_joined_day": [],
                "all_consistent": [],
                "all_vital": [],
                "all_active": [],
                "all_connected": [],
                "all_paused": [],
                "all_new_disengaged": ["user1", "user2"],
                "all_disengaged": [],
                "all_unpaused": [],
                "all_returned": [],
                "all_new_active": [],
                "all_still_active": [],
                "all_dropped": [],
                "all_disengaged_were_newly_active": [],
                "all_disengaged_were_consistently_active": [],
                "all_disengaged_were_vital": [],
                "all_lurker": [],
                "all_about_to_disengage": [],
                "all_disengaged_in_past": [],
            },
            {
                "date": date_two_past_days,
                "all_joined": [],
                "all_joined_day": [],
                "all_consistent": [],
                "all_vital": [],
                "all_active": [],
                "all_connected": [],
                "all_paused": [],
                "all_new_disengaged": ["user3", "user6", "user9"],
                "all_disengaged": [],
                "all_unpaused": [],
                "all_returned": [],
                "all_new_active": [],
                "all_still_active": [],
                "all_dropped": [],
                "all_disengaged_were_newly_active": [],
                "all_disengaged_were_consistently_active": [],
                "all_disengaged_were_vital": [],
                "all_lurker": [],
                "all_about_to_disengage": [],
                "all_disengaged_in_past": [],
            },
        ]
    )

    queue_name = "DISCORD_BOT"
    notifier = EngagementNotifier()
    notifier.notify_disengaged(guildId)

    queue = notifier.rabbitmq.channel.queue_declare(
        queue=queue_name,
        durable=True,
    )
    print(queue.method)

    # we had two users
    assert queue.method.consumer_count == 2

    # just deleting the queue after the test
    notifier.rabbitmq.channel.queue_delete(queue_name)
