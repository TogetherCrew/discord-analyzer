import unittest

from tc_analyzer_lib.automation.utils.interfaces import (
    AutomationAction,
    AutomationReport,
    AutomationTrigger,
)


class TestAutomationTrigger(unittest.TestCase):
    def test_automation_trigger_trigger(self):
        at_trigger = AutomationTrigger(
            options={"category": "all_new_disengaged"}, enabled=True
        )

        self.assertEqual(at_trigger.options, {"category": "all_new_disengaged"})
        self.assertEqual(at_trigger.enabled, True)


class TestAutomationAction(unittest.TestCase):
    def test_automation_actio_true_enabled(self):
        at_action = AutomationAction(
            template="hey {{username}}! please get back to us!",
            options={},
            enabled=True,
        )

        self.assertEqual(at_action.template, "hey {{username}}! please get back to us!")
        self.assertEqual(at_action.options, {})
        self.assertEqual(at_action.enabled, True)

    def test_automation_actio_false_enabled(self):
        at_action = AutomationAction(
            template="hey {{username}}! please get back to us!",
            options={},
            enabled=False,
        )

        self.assertEqual(at_action.template, "hey {{username}}! please get back to us!")
        self.assertEqual(at_action.options, {})
        self.assertEqual(at_action.enabled, False)


class TestAutomationReport(unittest.TestCase):
    def test_automation_report_true_enabled(self):
        at_report = AutomationReport(
            recipientIds=["111"],
            template="hey {{username}}, this is a report!",
            options={},
            enabled=True,
        )

        self.assertEqual(at_report.recipientIds, ["111"])
        self.assertEqual(at_report.template, "hey {{username}}, this is a report!")
        self.assertEqual(at_report.options, {})
        self.assertEqual(at_report.enabled, True)

    def test_automation_report_flase_enabled(self):
        at_report = AutomationReport(
            recipientIds=["111", "112"],
            template="hey {{username}}, this is a report!",
            options={},
            enabled=False,
        )

        self.assertEqual(at_report.recipientIds, ["111", "112"])
        self.assertEqual(at_report.template, "hey {{username}}, this is a report!")
        self.assertEqual(at_report.options, {})
        self.assertEqual(at_report.enabled, False)
