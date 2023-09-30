import os
from dotenv import load_dotenv


class Config:
    _parser = None

    def __init__(self):
        load_dotenv()

    def get(self, section_name, option_name, type=str):
        return os.environ.get(section_name.upper() + '_' + option_name.upper())
