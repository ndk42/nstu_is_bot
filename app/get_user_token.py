from requests import post as req_post


def get_user_token(nstu_login, nstu_password):
    headers = {
        'Content-Type': 'application/json',
        'X-OpenAM-Username': nstu_login,
        'X-OpenAM-Password': nstu_password
    }
    r = req_post(
        'https://login.nstu.ru/ssoservice/json/authenticate', headers=headers)
    return r.json()['tokenId']
