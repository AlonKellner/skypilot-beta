import httpx
import requests
import os


def add_oauth_header(headers: dict[str, str]) -> dict[str, str]:
    if ("SKY_OAUTH_CLIENT_ID" in os.environ) and ("SKY_OAUTH_PROVIDER" in os.environ):
      client_id = os.environ["SKY_OAUTH_CLIENT_ID"]
      provider = os.environ["SKY_OAUTH_PROVIDER"]
      if provider.lower() == "google":
        import google.auth
        from google.auth.transport.requests import Request
        from google.oauth2 import id_token
        
        # Get credentials
        credentials, project = google.auth.default()
        
        # Get an OpenID Connect token
        if credentials.valid:
            credentials.refresh(Request())
        
        # Get the ID token
        open_id_token = id_token.fetch_id_token(Request(), client_id)
      
        headers['Authorization'] = f'Bearer {open_id_token}'
    return headers


def lazy_import_server_common():
    global server_common
    from sky.server import common as server_common


def add_oauth_to_server_headers(url, headers):
    lazy_import_server_common()
    if server_common.get_server_url() in url:
      headers = add_oauth_header(headers)
    return headers


def patch_requests():
    original_request = requests.request

    def patched_request(method, url, *args, **kwargs):
        headers = kwargs.get('headers', {})
        headers = add_oauth_to_server_headers(url, headers)
        kwargs['headers'] = headers
        return original_request(method, url, *args, **kwargs)

    requests.request = patched_request


def patch_httpx():
    original_request = httpx.Client.request

    def patched_request(self, method, url, *args, **kwargs):
        headers = kwargs.get('headers', {})
        headers = add_oauth_to_server_headers(url, headers)
        kwargs['headers'] = headers
        return original_request(self, method, url, *args, **kwargs)

    httpx.Client.request = patched_request


def patch_http_clients():
    patch_requests()
    patch_httpx()


patch_http_clients()
