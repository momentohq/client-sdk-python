from . import header_client_interceptor

def get_authorization_interceptor(auth_token):
    return header_client_interceptor.header_adder_interceptor('authorization', auth_token)
