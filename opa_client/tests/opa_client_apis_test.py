import unittest
import json
from requests import exceptions
from http import HTTPStatus

from opa_client.opa_client_apis import OPAClient, Policy, Data


class OPAClientApiTest(unittest.TestCase):
    def setUp(self):
        """
        This function prepares the system for running tests
        """
        pass

    def tearDown(self):
        pass

    def test_create_opa_base_doc(self):
        client = OPAClient()
        data = {
            "users": [
                {"alice": 1},
                {"bob": 2}
            ]
        }
        resp = client.create_opa_base_doc("test_data", json.dumps(data))
        self.assertEqual(resp.data_name, "test_data")
