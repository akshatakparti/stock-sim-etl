import os
from dotenv import load_dotenv

# load environment variables from .env
load_dotenv()

def get_alpaca_keys():

    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    # raise error if no keys
    if not api_key or not secret_key:
        raise EnvironmentError("Missing Alpaca API keys in environment.")

    return {"api_key":api_key,"secret_key":secret_key}