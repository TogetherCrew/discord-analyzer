from unittest import TestCase

from tc_analyzer_lib.schemas import GraphSchema


class TestGraphSchema(TestCase):
    def setUp(self):
        self.platform_id = "51515515151515"

    def test_just_platform_name_given(self):
        graph = GraphSchema(
            platform="discord",
        )

        self.assertEqual(graph.platform_label, "DiscordPlatform")
        self.assertEqual(graph.user_label, "DiscordMember")
        self.assertEqual(graph.interacted_in_rel, "INTERACTED_IN")
        self.assertEqual(graph.interacted_with_rel, "INTERACTED_WITH")
        self.assertEqual(graph.member_relation, "IS_MEMBER")

    def test_platform_name_contain_space(self):
        with self.assertRaises(ValueError):
            _ = GraphSchema(
                platform="my discord",
            )

    def test_platform_name_contain_underline(self):
        with self.assertRaises(ValueError):
            _ = GraphSchema(
                platform="my_discord",
            )

    def test_given_all_inputs(self):
        graph = GraphSchema(
            platform="telegram",
            interacted_in_rel="INTERACTED_IN_1",
            interacted_with_rel="INTERACTED_WITH_2",
            member_relation="IS_MEMBER_3",
        )

        self.assertEqual(graph.platform_label, "TelegramPlatform")
        self.assertEqual(graph.user_label, "TelegramMember")
        self.assertEqual(graph.interacted_in_rel, "INTERACTED_IN_1")
        self.assertEqual(graph.interacted_with_rel, "INTERACTED_WITH_2")
        self.assertEqual(graph.member_relation, "IS_MEMBER_3")
