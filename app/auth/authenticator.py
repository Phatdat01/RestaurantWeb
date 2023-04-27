import yaml
from yaml.loader import SafeLoader

import streamlit as st
import streamlit_authenticator as stauth


class Auth:
    def __init__(self):
        self._authenticator = self._auth()

    def _auth(self):
        '''
        Make authenticator object from config.yaml

        :param config: config.yaml

        :return: authenticator object

        '''

        with open('/home/nguynmanh/works/recommender/RestaurantWeb/config/config.yaml') as file:
            config = yaml.load(file, Loader=SafeLoader)

        authenticator = stauth.Authenticate(
            config['credentials'],
            config['cookie']['name'],
            config['cookie']['key'],
            config['cookie']['expiry_days'],
            config['preauthorized']
        )

        return authenticator
    

    def login(self) -> tuple:
        '''
        Login page for user to login to system and get username and password from user input 

        :param button_text: text of button
        :param key: key of button

        :return: name, authentication_status, username

        '''
        hashed_passwords = stauth.Hasher(['abc', 'def']).generate()

        name, authentication_status, username = self._authenticator.login('Login', 'main')

        return name, authentication_status, username

    def logout(self, button_text: str, key: str) -> None:
        '''
        Logout page for user to logout from system

        :param button_text: text of button
        :param key: key of button

        :return: None

        '''
        self._authenticator.logout(button_text, key)


    def register(self) -> tuple:
        '''
        Register page for user to register to system and get username and password from user input

        :param usename: username of user
        :param password: password of user

        :return: None

        '''

        name, authentication_status, username = self._authenticator.register('Register', 'main')

        return name, authentication_status, username