import requests, os
from dotenv import load_dotenv

load_dotenv()

email = os.getenv('MY_EMAIL')
base_api = os.getenv('BASE_API')

url = '{base_api}/{endpoint}'

params = {
    'email': email
}

res = requests.post(url=url.format(base_api=base_api,endpoint='request-access-code'),params=params)
print(res.status_code)
print(res.text)
my_code = input('Enter auth code: ')
params = {
    'email': email,
    'accessCode': my_code
}
res = requests.post(url=url.format(base_api=base_api,endpoint='authenticate'),params=params)
print(res.status_code)
print(res.text)