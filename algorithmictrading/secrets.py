from dotenv import load_dotenv
import os

env_path = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))),'.env') 

load_dotenv(env_path)

IEX_CLOUD_API_TOKEN = os.getenv('IEX_CLOUD_API_TOKEN')