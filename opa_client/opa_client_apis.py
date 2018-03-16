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


class Policy(object):

    def __init__(self, policy_name, policy_url, data_packages, policy_version=1):
        """
        :param policy_name: Name of the policy
        :param policy_url: Policy url
        :param data_packages: List of base documents used in the policy
        :param policy_version: (optional) policy version
        :type policy_name: str
        :type policy_url: str
        :type data_packages: list
        :type policy_version: int
        """
        self.policy_name = policy_name
        self.policy_url = policy_url
        self.data_packages = data_packages
        self.policy_version = policy_version

    def get_policy_name(self):
        """ Returns Policy Name"""
        return self.policy_name

    def get_policy_url(self):
        """ Returns Policy url"""
        return self.policy_url

    def get_policy_version(self):
        """ Returns Policy version"""
        return self.policy_version

    @known_exceptions
    def delete_policy(self):
        """ This function deletes the policy from the OPA server"""
        resp = RestClientApis.http_delete_and_check_success(self.policy_url + self.policy_name)
        if resp.http_status != HTTPStatus.OK:
            raise exceptions.HTTPError(resp.message)
        return True, "Policy Deleted"


class Data(object):
    def __init__(self, data_name, data_url, json_data):
        self.data_name = data_name
        self.data_url = data_url
        self.json_data = json_data

    def get_data_name(self):
        """ Returns Data Name"""
        return self.data_name

    def get_data_url(self):
        """ Returns Data Url"""
        return self.data_url

    def get_json_data(self):
        """ Returns JSON data of the Base Document"""
        return self.json_data

    @known_exceptions
    def base_doc_add(self, path, value):

        data = [
            {
                'op': 'add',
                'path': path,
                'value': value
            }
        ]
        resp = RestClientApis.http_patch_and_check_success(self.data_url + self.data_name + path, json.dumps(data))
        if not resp.success:
            raise FileNotFoundError(resp.message)
        return True, "Added successfully"

    @known_exceptions
    def base_doc_remove(self, path):

        data = [
            {
                'op': 'remove',
                'path': path
            }
        ]
        resp = RestClientApis.http_patch_and_check_success(self.data_url + self.data_name + path, json.dumps(data))
        if not resp.success:
            raise FileNotFoundError(resp.message)
        return True, "Removed successfully"


class OPAClient(object):

    def __init__(self, server='localhost', port=8181, version='v1'):
        """
        :param server: OPA server address
        :param port: OPA server port
        :param version: OPA server version
        :type server: str
        :type port: int
        :param version: str
        """
        self.server = server
        self.port = port
        self.version = version

    @known_exceptions
    def create_opa_policy(self, policy_name, policy=None, *args):
        """
        This function creates a OPA policy on the server
        :param policy_name: Name of the policy
        :param policy: Policy allow conditions as String
        :return: Rest Return
        :type policy_name: str
        :type policy: str
        """
        url = 'http://' + self.server + ':' + str(self.port) + '/' + self.version + '/policies/'
        data = ''
        data_names = list()

        name = policy_name.replace('/', '.') if '/' in policy_name else policy_name
        data = data + 'package ' + name + '\n'
        for arg in args:
            data_names.append(arg)
            data = data + 'import data.' + arg + '\n'
        data = data + 'default allow = False \n'
        data = data + 'allow { \n'
        data = data + policy
        data = data + '\n }'

        policy_resp = RestClientApis.http_put_and_check_success(url + policy_name, data,
                                                                headers={'Content-Type': 'text/plain'})
        if not policy_resp.success:
            raise exceptions.InvalidURL(policy_resp.message)

        return Policy(policy_name, url, data_names)

    @known_exceptions
    def create_opa_base_doc(self, base_doc_name, json_data):
        """
        This function creates a OPA policy on the server
        :param base_doc_name: Name of the base document
        :param json_data: Json data for Base Document
        :return: Rest Return
        """
        url = 'http://' + self.server + ':' + str(self.port) + '/' + self.version + '/data/'
        resp = RestClientApis.http_put_and_check_success(url + base_doc_name, json_data,
                                                         headers={'Content-Type': 'application/json'})
        if not resp.success:
            raise exceptions.InvalidURL(resp.message)
        return Data(base_doc_name, url, json_data)
