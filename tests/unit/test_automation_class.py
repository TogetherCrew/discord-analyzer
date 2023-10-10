import unittest

from automation.utils.interfaces import (
    Automation,
    AutomationTrigger,
    AutomationAction,
    AutomationReport,
)


class TestAutomation(unittest.TestCase):
    def test_automation(self):
        triggers = [
            AutomationTrigger(
                options={"category": "all_new_disengaged"}, enabled=False
            ),
            AutomationTrigger(options={"category": "all_new_active"}, enabled=True),
        ]
        actions = [
            AutomationAction(
                template="hey {{username}}! please get back to us!",
                options={},
                enabled=True,
            ),
            AutomationAction(
                template="hey {{username}}! please get back to us2!",
                options={},
                enabled=False,
            ),
        ]

        report = AutomationReport(
            recipientIds=["111"],
            template="hey {{username}}, this is a report!",
            options={},
            enabled=True,
        )
        automation = Automation("123", triggers, actions, report, enabled=True)

        self.assertIsInstance(automation.triggers[0], AutomationTrigger)
        self.assertEqual(
            automation.triggers[0].options,
            {"category": "all_new_disengaged"},
        )
        self.assertEqual(
            automation.triggers[0].enabled,
            False,
        )

        self.assertIsInstance(automation.triggers[1], AutomationTrigger)
        self.assertEqual(
            automation.triggers[1].options,
            {"category": "all_new_active"},
        )
        self.assertEqual(
            automation.triggers[1].enabled,
            True,
        )

        self.assertIsInstance(automation.actions[0], AutomationAction)
        self.assertEqual(
            automation.actions[0].template,
            "hey {{username}}! please get back to us!",
        )
        self.assertEqual(
            automation.actions[0].options,
            {},
        )
        self.assertEqual(
            automation.actions[0].enabled,
            True,
        )

        self.assertIsInstance(automation.actions[1], AutomationAction)
        self.assertEqual(
            automation.actions[1].template,
            "hey {{username}}! please get back to us2!",
        )
        self.assertEqual(
            automation.actions[1].options,
            {},
        )
        self.assertEqual(
            automation.actions[1].enabled,
            False,
        )

    def test_to_dict(self):
        triggers = [
            AutomationTrigger(
                options={"category": "all_new_disengaged"}, enabled=False
            ),
            AutomationTrigger(options={"category": "all_new_active"}, enabled=True),
        ]
        actions = [
            AutomationAction(
                template="hey {{username}}! please get back to us!",
                options={},
                enabled=True,
            ),
            AutomationAction(
                template="hey {{username}}! please get back to us2!",
                options={},
                enabled=False,
            ),
        ]

        report = AutomationReport(
            recipientIds=["111"],
            template="hey {{username}}, this is a report!",
            options={},
            enabled=True,
        )
        automation = Automation("123", triggers, actions, report, enabled=True)

        automation_dict = automation.to_dict()

        self.assertEqual(automation_dict["guildId"], "123")

        self.assertEqual(
            automation_dict["triggers"],
            [
                {"options": {"category": "all_new_disengaged"}, "enabled": False},
                {"options": {"category": "all_new_active"}, "enabled": True},
            ],
        )

        self.assertEqual(
            automation_dict["actions"],
            [
                {
                    "template": "hey {{username}}! please get back to us!",
                    "options": {},
                    "enabled": True,
                },
                {
                    "template": "hey {{username}}! please get back to us2!",
                    "options": {},
                    "enabled": False,
                },
            ],
        )

        self.assertEqual(
            automation_dict["report"],
            {
                "recipientIds": ["111"],
                "template": "hey {{username}}, this is a report!",
                "options": {},
                "enabled": True,
            },
        )
        self.assertEqual(automation_dict["enabled"], True)

    def test_from_dict(self):
        automation_dict = {
            "guildId": "123",
            "triggers": [
                {"options": {"category": "all_new_disengaged"}, "enabled": False},
                {"options": {"category": "all_new_active"}, "enabled": True},
            ],
            "actions": [
                {
                    "template": "hey {{username}}! please get back to us!",
                    "options": {},
                    "enabled": True,
                },
                {
                    "template": "hey {{username}}! please get back to us2!",
                    "options": {},
                    "enabled": False,
                },
            ],
            "report": {
                "recipientIds": ["111"],
                "template": "hey {{username}}, this is a report!",
                "options": {},
                "enabled": True,
            },
            "enabled": False,
        }

        automation = Automation.from_dict(automation_dict)

        self.assertIsInstance(automation.triggers[0], AutomationTrigger)
        self.assertEqual(
            automation.triggers[0].options,
            {"category": "all_new_disengaged"},
        )
        self.assertEqual(
            automation.triggers[0].enabled,
            False,
        )

        self.assertIsInstance(automation.triggers[1], AutomationTrigger)
        self.assertEqual(
            automation.triggers[1].options,
            {"category": "all_new_active"},
        )
        self.assertEqual(
            automation.triggers[1].enabled,
            True,
        )

        self.assertIsInstance(automation.actions[0], AutomationAction)
        self.assertEqual(
            automation.actions[0].template,
            "hey {{username}}! please get back to us!",
        )
        self.assertEqual(
            automation.actions[0].options,
            {},
        )
        self.assertEqual(
            automation.actions[0].enabled,
            True,
        )

        self.assertIsInstance(automation.actions[1], AutomationAction)
        self.assertEqual(
            automation.actions[1].template,
            "hey {{username}}! please get back to us2!",
        )
        self.assertEqual(
            automation.actions[1].options,
            {},
        )
        self.assertEqual(
            automation.actions[1].enabled,
            False,
        )