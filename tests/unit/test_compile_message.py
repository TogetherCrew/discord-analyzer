import unittest

from tc_analyzer_lib.automation.automation_workflow import AutomationWorkflow


class CompileTemplateMessage(unittest.TestCase):
    def test_action_compile_message(self):
        """
        test the templates that could be found in actions
        """
        template = "Hi {{username}}!"
        user_name = "user1"

        at_workflow = AutomationWorkflow()
        type = at_workflow._get_handlebar_type(template)
        compiled_message = at_workflow._compile_message(
            data={type: user_name}, message=template
        )

        self.assertEqual(compiled_message, "Hi user1!")

    def test_report_compile_message(self):
        """
        test the templates that could be found in reports
        """
        template = "This users were messaged\n{{#each usernames}}{{this}}{{/each}}"
        user_names = ["user1", "user2"]

        at_workflow = AutomationWorkflow()
        compiled_message = at_workflow._prepare_report_compiled_message(
            user_names, template
        )

        expected_message = "This users were messaged\n"
        expected_message += "- user1\n- user2\n"
        self.assertEqual(compiled_message, expected_message)
