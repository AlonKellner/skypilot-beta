import httpx
import requests
import os


def get_oauth_iap_token(client_id):
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request
    
    # Create a credentials object
    credentials = service_account.IDTokenCredentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        target_audience=client_id
    )
    
    # Request the token
    credentials.refresh(Request())
    id_token = credentials.token
    return id_token


def add_oauth_header(headers: dict[str, str]) -> dict[str, str]:
    if "SKY_OAUTH_PROVIDER" in os.environ:
      provider = os.environ["SKY_OAUTH_PROVIDER"]
      if provider.lower() == "google":
        client_id = os.environ["SKY_OAUTH_CLIENT_ID"]
        iap_token = get_oauth_iap_token(client_id)
        headers['Proxy-Authorization'] = f'Bearer {iap_token}'
    return headers


def lazy_import_server_common():
    global server_common
    from sky.server import common as server_common


def add_oauth_to_server_headers(url, headers):
    lazy_import_server_common()
    if server_common.get_server_url().strip("/") in url:
      headers = add_oauth_header(headers)
    return headers


def patch_requests():
    original_request = requests.request

    def patched_request(method, url, *args, **kwargs):
        headers = kwargs.get('headers', {})
        headers = add_oauth_to_server_headers(url, headers)
        kwargs['headers'] = headers
        response = original_request(method, url, *args, **kwargs)
        content_type = response.headers.get('Content-Type', '').lower()
        if response.history and "text/html" in content_type:
            headers = add_oauth_header(headers)
            kwargs['headers'] = headers
            response = original_request(method, url, *args, **kwargs)
        return response


    requests.request = patched_request
    requests.api.request = patched_request


def patch_httpx():
    original_request = httpx.Client.request

    def patched_request(self, method, url, *args, **kwargs):
        headers = kwargs.get('headers', {})
        headers = add_oauth_to_server_headers(url, headers)
        kwargs['headers'] = headers
        response = original_request(self, method, url, *args, **kwargs)
        content_type = response.headers.get('Content-Type', '')
        if response.history and "text/html" in content_type:
            headers = add_oauth_header(url, headers)
            kwargs['headers'] = headers
            response = original_request(self, method, url, *args, **kwargs)
        return response

    httpx.Client.request = patched_request


def patch_http_clients():
    patch_requests()
    patch_httpx()


patch_http_clients()
