import unittest

from tc_analyzer_lib.automation.automation_workflow import AutomationWorkflow


class TestGetHandlerType(unittest.TestCase):
    def test_empty_string(self):
        template = ""
        at_workflow = AutomationWorkflow()
        type = at_workflow._get_handlebar_type(template)

        self.assertEqual(type, None)

    def test_sample_string(self):
        template = "Hi {{username}}!"

        at_workflow = AutomationWorkflow()
        type = at_workflow._get_handlebar_type(template)

        self.assertEqual(type, "username")

    def test_sample_string_multiple_handlebar(self):
        """
        for now we're supporting have the only first handlebar
        """
        template = "Hello {{username}} and {{nickname}}!"

        at_workflow = AutomationWorkflow()
        type = at_workflow._get_handlebar_type(template)

        self.assertEqual(type, "username")
