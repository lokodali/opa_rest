import logging
import os
import json
import glob
from http import HTTPStatus
from requests import exceptions

from magen_logger.logger_config import LogDefaults
from magen_rest_apis.rest_client_apis import RestClientApis
from magen_rest_apis.rest_return_api import RestReturn
from opa_client.opa_exception_apis import handle_specific_exception

logger = logging.getLogger(LogDefaults.default_log_name)


def known_exceptions(func):
    """
    Known Exceptions decorator.
    wraps a given function into try-except statement
    :param func: function to decorate
    :type func: Callable
    :return: decorated
    :rtype: Callable
    """

    def helper(*args, **kwargs):
        """Actual Decorator for handling known exceptions"""
        try:
            return func(*args, **kwargs)
        except (exceptions.RequestException, FileExistsError, FileNotFoundError, EOFError, IndexError,
                json.JSONDecodeError) as err:
            return handle_specific_exception(err)
        except TypeError as err:
            success = False
            return RestReturn(success=success, message=err.args[0])

    return helper


class OPAClient(object):

    def __init__(self, server='localhost', port=8181):
        self.server = server
        self.port = port

    @known_exceptions
    def create_opa_policy(self, policy_name, data_files, policy=None):
        """
        This function creates a OPA policy on the server
        :param policy_name: Name of the policy
        :param data_files: List of data files used in the policy
        :param policy: Policy allow conditions as String
        :return: Rest Return
        """
        policy_resp = None
        url = 'http://' + self.server + ':' + str(self.port) + 'v1/policies/' + policy_name
        dir_path = os.path.dirname(os.path.realpath(__file__))
        file_path = dir_path + '/' + policy_name + '.rego'

        with open(file_path, 'w') as file:
            file.write('package ' + policy_name + '\n')
            file.write('import input as http_api \n')
            for i in data_files:
                file.write('import data.' + i + '\n')
            file.write('default allow = False \n')
            file.write('allow { \n')
            file.write(policy)
            file.write('\n }')

        with open(file_path, 'r') as file:
            policy_data = file.read()
            policy_resp = RestClientApis.http_put_and_check_success(url, policy_data,
                                                                    headers={'Content-Type': 'text/plain'},
                                                                    params={'file': file_path})
        return policy_resp

    @known_exceptions
    def create_opa_base_doc(self, base_doc_name, json_data):
        """
        This function creates a OPA policy on the server
        :param base_doc_name: Name of the base document
        :param json_data: Json data for Base Document
        :return: Rest Return
        """
        url = 'http://' + self.server + ':' + str(self.port) + 'v1/data/' + base_doc_name
        resp = RestClientApis.http_put_and_check_success(url, json_data, headers={'Content-Type': 'application/json'})
        return resp

    @known_exceptions
    def delete_policy(self, policy_name):
        url = 'http://' + self.server + ':' + str(self.port) + 'v1/policies/' + policy_name
        resp = RestClientApis.http_delete_and_check_success(url)
        if resp.http_status != HTTPStatus.OK:
            raise exceptions.HTTPError(resp.message)
        return resp

    @known_exceptions
    def base_doc_add(self, path, value):
        url = 'http://' + self.server + ':' + str(self.port) + 'v1/data/'
        data = [
            {
                'op': 'add',
                'path': path,
                'value': value
            }
        ]
        resp = RestClientApis.http_patch_and_check_success(url + path, json.dumps(data))
        return resp

    @known_exceptions
    def base_doc_remove(self, path):
        url = 'http://' + self.server + ':' + str(self.port) + 'v1/data/'
        data = [
            {
                'op': 'remove',
                'path': path
            }
        ]
        resp = RestClientApis.http_patch_and_check_success(url + path, json.dumps(data))
        return resp
