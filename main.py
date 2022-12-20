import os

tw_access_token = os.environ.get("TW_ACCESS_TOKEN")
assert tw_access_token and len(tw_access_token) > 0, 'TW_ACCESS_TOKEN is not set'
