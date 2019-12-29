import facebook

import os
import requests

FACEBOOK_APP_ID = os.environ.get('FACEBOOK_APP_ID', '')
FACEBOOK_APP_SECRET = os.environ.get('FACEBOOK_APP_SECRET', '')
FACEBOOK_APP_TOKEN = os.environ.get('FACEBOOK_APP_TOKEN', '')

class Facebook:
    def __init__(self, app_id=FACEBOOK_APP_ID, app_secret=FACEBOOK_APP_SECRET,
                 app_token=FACEBOOK_APP_TOKEN):
        self.app_id = app_id
        self.app_secret = app_secret
        self.app_token = app_token
        self.url = "https://graph.facebook.com"
        self.version = "3.1"

    def create_access_token(self):
        response = requests.get(
            url=f"{self.url}/oauth/access_token?client_id={self.app_id}&client_secret={self.app_secret}&grant_type=client_credentials",
            headers={
                "Content-Type": "application/json",
            }
        )

        response.raise_for_status()

        return response.json()['access_token']

    @property
    def graph_api(self):
        graph_api = facebook.GraphAPI(
            access_token=self.app_token, 
            version=self.version)

        return graph_api

    def who_am_i(self):
        profile = self.graph_api.get_object("me")
        print(profile)

    def feed(self, id="me"):
        response = requests.get(
            url=f"{self.url}/v{self.version}/{id}/feed?access_token={self.app_token}"
        )

        print(response.json())

if __name__ == "__main__":
    fb_client = Facebook().who_am_i()