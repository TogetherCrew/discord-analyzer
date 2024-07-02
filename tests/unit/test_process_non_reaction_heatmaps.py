from unittest import TestCase

import numpy as np
from tc_analyzer_lib.algorithms.compute_interaction_matrix_discord import (
    process_actions,
)


class TestProcessNonReactions(TestCase):
    def test_empty_inputs(self):
        intput_data = {}
        results = process_actions(heatmaps_data_per_acc=intput_data, skip_fields=[])
        self.assertEqual(results, {})

    def test_single_account_no_action(self):
        # 24 hours
        zeros_vector = np.zeros(24)
        input_data = {
            "acc1": [
                {
                    "lone_messages": zeros_vector,
                    "thr_messages": zeros_vector,
                    "reacted_per_acc": [
                        [{"account": "acc2", "count": 1}],
                        [{"account": "acc3", "count": 5}],
                    ],
                    "replied_per_acc": [],
                    "date": "2024-01-01",
                }
            ]
        }
        results = process_actions(
            input_data, skip_fields=["date", "reacted_per_acc", "replied_per_acc"]
        )

        expected_results = {
            "acc1": [
                {
                    "lone_messages": [],
                    "thr_messages": [],
                    # others same as before
                    "reacted_per_acc": [
                        [{"account": "acc2", "count": 1}],
                        [{"account": "acc3", "count": 5}],
                    ],
                    "replied_per_acc": [],
                    "date": "2024-01-01",
                }
            ]
        }
        self.assertEqual(results, expected_results)

    def test_single_account_with_action(self):
        lone_messages = np.zeros(24)
        # 3 channel messages at hour 6
        lone_messages[5] = 3

        thr_messages = np.zeros(24)
        thr_messages[1] = 1

        input_data = {
            "acc1": [
                {
                    "lone_messages": lone_messages,
                    "thr_messages": thr_messages,
                    "reacted_per_acc": [
                        {"account": "acc2", "count": 1},
                        {"account": "acc3", "count": 5},
                    ],
                    "replied_per_acc": [],
                    "date": "2024-01-01",
                }
            ]
        }
        results = process_actions(
            input_data, skip_fields=["date", "replied_per_acc", "reacted_per_acc"]
        )
        expected_results = {
            "acc1": [
                {
                    "lone_messages": [{"account": "acc1", "count": 3}],
                    "thr_messages": [{"account": "acc1", "count": 1}],
                    # others same as before
                    "reacted_per_acc": [
                        {"account": "acc2", "count": 1},
                        {"account": "acc3", "count": 5},
                    ],
                    "replied_per_acc": [],
                    "date": "2024-01-01",
                }
            ]
        }
        self.assertEqual(results, expected_results)

    def test_multiple_account_with_action(self):
        user1_lone_messages = np.zeros(24)
        # 3 channel messages from hour 6 to 7
        user1_lone_messages[5] = 3

        user1_thr_messages = np.zeros(24)
        user1_thr_messages[1] = 1

        user2_thr_messages = np.zeros(24)
        user2_thr_messages[7] = 5
        user2_thr_messages[20] = 2

        input_data = {
            "acc1": [
                {
                    "lone_messages": user1_lone_messages,
                    "thr_messages": user1_thr_messages,
                    "reacted_per_acc": [
                        {"account": "acc2", "count": 1},
                        {"account": "acc3", "count": 5},
                    ],
                    "replied_per_acc": {},
                    "date": "2024-01-01",
                }
            ],
            "acc2": [
                {
                    "lone_messages": np.zeros(24),
                    "thr_messages": user2_thr_messages,
                    "reacted_per_acc": [
                        {"account": "acc5", "count": 3},
                    ],
                    "replied_per_acc": [],
                    "date": "2024-01-01",
                }
            ],
        }
        results = process_actions(
            input_data, skip_fields=["date", "replied_per_acc", "reacted_per_acc"]
        )

        expected_results = {
            "acc1": [
                {
                    "lone_messages": [{"account": "acc1", "count": 3}],
                    "thr_messages": [{"account": "acc1", "count": 1}],
                    # others same as before
                    "reacted_per_acc": [
                        {"account": "acc2", "count": 1},
                        {"account": "acc3", "count": 5},
                    ],
                    "replied_per_acc": {},
                    "date": "2024-01-01",
                }
            ],
            "acc2": [
                {
                    "lone_messages": [],
                    "thr_messages": [{"account": "acc2", "count": 7}],
                    # others same as before
                    "reacted_per_acc": [
                        {"account": "acc5", "count": 3},
                    ],
                    "replied_per_acc": [],
                    "date": "2024-01-01",
                }
            ],
        }
        self.assertEqual(results, expected_results)

    def test_multiple_account_multiple_documents_with_action(self):
        user1_lone_messages = np.zeros(24)
        # 3 channel messages from hour 6 to 7
        user1_lone_messages[5] = 3

        user1_thr_messages = np.zeros(24)
        user1_thr_messages[1] = 1

        user2_thr_messages = np.zeros(24)
        user2_thr_messages[7] = 5
        user2_thr_messages[20] = 2

        input_data = {
            "acc1": [
                {
                    "lone_messages": user1_lone_messages,
                    "thr_messages": user1_thr_messages,
                    "reacted_per_acc": [
                        {"account": "acc2", "count": 1},
                        {"account": "acc3", "count": 5},
                    ],
                    "replied_per_acc": {},
                    "date": "2024-01-01",
                },
                {
                    "lone_messages": np.zeros(24),
                    "thr_messages": user1_lone_messages,
                    "reacted_per_acc": [
                        {"account": "acc2", "count": 1},
                        {"account": "acc3", "count": 5},
                    ],
                    "replied_per_acc": {},
                    "date": "2024-01-02",
                },
            ],
            "acc2": [
                {
                    "lone_messages": np.zeros(24),
                    "thr_messages": user2_thr_messages,
                    "reacted_per_acc": [
                        {"account": "acc5", "count": 3},
                    ],
                    "replied_per_acc": [],
                    "date": "2024-01-01",
                }
            ],
        }
        results = process_actions(
            input_data, skip_fields=["date", "reacted_per_acc", "replied_per_acc"]
        )

        expected_results = {
            "acc1": [
                {
                    "lone_messages": [{"account": "acc1", "count": 3}],
                    "thr_messages": [{"account": "acc1", "count": 1}],
                    # others same as before
                    "reacted_per_acc": [
                        {"account": "acc2", "count": 1},
                        {"account": "acc3", "count": 5},
                    ],
                    "replied_per_acc": {},
                    "date": "2024-01-01",
                },
                {
                    "lone_messages": [],
                    "thr_messages": [{"account": "acc1", "count": 3}],
                    # others same as before
                    "reacted_per_acc": [
                        {"account": "acc2", "count": 1},
                        {"account": "acc3", "count": 5},
                    ],
                    "replied_per_acc": {},
                    "date": "2024-01-02",
                },
            ],
            "acc2": [
                {
                    "lone_messages": [],
                    "thr_messages": [{"account": "acc2", "count": 7}],
                    # others same as before
                    "reacted_per_acc": [
                        {"account": "acc5", "count": 3},
                    ],
                    "replied_per_acc": [],
                    "date": "2024-01-01",
                }
            ],
        }
        self.assertEqual(results, expected_results)
