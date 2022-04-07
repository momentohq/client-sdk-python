import unittest

from momento.momento_signer import MomentoSigner, CacheOperation, SigningRequest
from momento.errors import InvalidArgumentError
import jwt
from jwt.api_jwk import PyJWK

_RSA_256_JWK = '{"p":"4Xhu9y3DCDO6ttlDrb5JuSk9-F6oKIE7y3zDxObR1UbLBPua4X3qW82VGYjzn6yO7_qRECzN-K1LLd6yIwK5L7i6rIglP_z-Kyzp-UwacQrnvvf0ZM1xE2E6JUAkevXf1khCXAuKd04S9NxEKv0gDvwdspw-WwqiOMVU1gn1aF0","kty":"RSA","q":"0Pyj5MqoVGuUSGYL0-HLYBspD7vgi9G8HjNh8MQC1AF5w58k29dtAH2-dj3IO3b_CrNisKyMpYLT1OJBsYaJGi2TZRnBGbZb-LyZTz5vDBu8f3SgUxSHcqUsVOO7Pr555OJKdNRr3ag3m8RwIn3Agbvrs0HQPM-XkfUI7p7Bc28","d":"EqUgXXSG8gWx2wGJpjKZTRMd7lzuct96AyzWBsCTmYAVj6BsczKERFUFtoUs6kgF4aZiq8ym-mNQdVvane6aeK3p0F5RfvGwtGg7xkavNidtOf51fEPa8yCXLIwFclM_tav4Y_LlGdiXNsXVX6X8ZgTiS5vUCRzt_5e8Nks1T1kMPFdiW_9F-emXWrl5hmrIiG1XpBX4sG6YNGWty1u7Reu9a8ydzMmcFSuxE5bJodDPE1ZiZOQOtVxk3TVAKUEQ7SqOksN_w_FX_6Du5bC8cjz38dJhfL3R9B_FQA5oM_nO1XklgigF8lg8_b2sTb-OQc6dY6qE241ZknwxDbKIIQ","e":"AQAB","kid":"testKeyId","qi":"0NHMRRg_aQT8hKItg1HxnUqgWHHb4S92DqhBY5AERmOPUUQvK7qkpc0DN4OVQwZGpl0_lBqsadjoCROrJ6HrVMkx0fkoNPQIKRzT_ew6ORXPPrB5d2elS4c93mjYZjvQtxdpQKtyM4_9Va1v6PRa2IGUSPzJt6h8BQBM9NQhHuY","dp":"aWizgA2942TDwt46HM0cjFsypJ4kQaOBf_WZVMGQkgQhv_edBhSm7zpinWiAdULoJFthXE2GEd96iTxWzbVlPGFBrI2N1KeDcE30KN-icPznMUmc0U-WsLfAxk-BfpbaicSIeZ3Po0014ZHksLBcP4UwoSMYp9mF08K1kcdgGuU","alg":"RS256","dq":"DsvUTr6KbG-xb-7Jp5a073j8z0BeBYgz6W9537IBAUGZfWAnG-mEriQ49-Yn5w3lwLwyoI-W5aD9nnTmccs0qcXQSbgpE8j1egbgU9v3wMO19NAtCbTKYjOPj_MPrsGNn8blvp_Lg0YFqeGejtKYbpb_eRGPzL5l3M-cckiLKcE","n":"uBBdD0DnAeArN2cqaG9zUDmZU_hEen7glZN0ssVSf_ZML_HdEu7BR5G9gGToYQ7dkH010FzciZMbouXUDovd71snXqlvnwyvqpU3UUfVgudmN-CDa97170WnjpjIQLVErQCX-3_PIctJweDmJLzaQsmQCPoSzZkiQ17NsMwN-xw2L-eRfwmyOWKv0mRkDpZRYgn4rrm70hepOZp0-YEpEC_vTYykc2WVuvEVez_9nF-VwXGVX-a1_zChIAz5V_-VXIqtoL7ot945wAEyEhtV4Yqr5LoSL8mvSL3v7WlF56NqaltDWe2yrowlZiQ2EDwwUKxArcYd7BnNmdYqo2cHUw"}'
_RSA_256_JWK_PUB = '{"kty":"RSA","e":"AQAB","kid":"testKeyId","alg":"RS256","n":"uBBdD0DnAeArN2cqaG9zUDmZU_hEen7glZN0ssVSf_ZML_HdEu7BR5G9gGToYQ7dkH010FzciZMbouXUDovd71snXqlvnwyvqpU3UUfVgudmN-CDa97170WnjpjIQLVErQCX-3_PIctJweDmJLzaQsmQCPoSzZkiQ17NsMwN-xw2L-eRfwmyOWKv0mRkDpZRYgn4rrm70hepOZp0-YEpEC_vTYykc2WVuvEVez_9nF-VwXGVX-a1_zChIAz5V_-VXIqtoL7ot945wAEyEhtV4Yqr5LoSL8mvSL3v7WlF56NqaltDWe2yrowlZiQ2EDwwUKxArcYd7BnNmdYqo2cHUw"}'
_RSA_384_JWK = '{"p":"2UqxTsQeIH2wsAP7FsDltFNGkco526v1nYjTSguReJr11klvMjju5H2cm6vrJGURQ6-e_yvyAzM7avG-D2o7ZGrYQ-pJXwF-lS7Nzwm7XWLvNv78vyH95aIdWh1KaeaBswN-JbGyamcmGgtwO_0ICTOMmyGpku38b52CHVgq-Dc","kty":"RSA","q":"0fF9Fnv2CHso23K13AcZCf5eTFy2tk9zd3zhheq_IbnvEjPiMp_UXAozd3Lh8MZMXpM0HhB1Zpxs89eF8Pyy3GwWq08F2lMOxDGaYxeTU258Kq2KFPQ8VDsY6SGuAUAGkwYM_IXX0_ixt3FqrJIAuLI9pPAkymV9Hi_oq-FOopM","d":"SNx9spekPEb3VY9gOuD4HPE9BkLbG-dAgTVgKYuhG5xqHLeHBqB7Y9k7U1fDoLOszV59yQjM9mQXhjWa0bGFpYAl_U_i3eSw62Fh3b5NG1mkbJL2Vc0QhPtmH7X7rQOL01PxyJ83RDQ08xlC5vcMv_cAfIICqxo-qsOCnEN9_6sUrPQkv19oakwfMq9ACXDWPgkHNwWTBbyO7CtUCf6chYLPr13MsjLe_FSqY0rrNKr68qq0WtXFaldJR1SeE_lEd7pR1kAMoX0rm57yNqBljsgMweOLMz6swulIRZBWtmEfxvvYL0H5RnmwSbz00NSjErqwv518KPpNzS9O8qE-dw","e":"AQAB","kid":"testKeyId","qi":"nxcUfqj-jKFtGmGKIQo8b4M3eOnXdpm0QWyfgazzk_v1uhpbGg_VC7ZKxqY1DMfQHyjxFEz8a3mFsusVvwsCFuSzmqp36-f7tuXiAVfJjHs8cNIVZbO0GU1iRzd21yfBuj1JIr2FoU2g_1G--h48-tWXOmrPmBc9_xTSfjEeNGE","dp":"DiCqGIntv4UMiNUpbRhLlwbXDsGMM3khtgVgX28THTlOBImvvh8vgRGdrg1mc25SygjQGJ0d1hFtqo1fIxdwFx5PQ1MnRBMPzNlHLk_eq7qz_OplOnQWUujQabx_yxTel-oBOKguBncAZi8aM_xGmnqMiMWOhewNPqCKBihmWs8","alg":"RS384","dq":"zV2YqyHfbjRrpx7y3qTizW_R9ojLAlN98-hpA4K6LNehEQFHx5WpOc-QwMvUUJ7pnaoJVU9sSE_EFFNDZpUKsavaEQFgDE0rKKgNCdnJ99cgBu9zH0Q6r3qPx512hSqIQ9GramnS0jt4PKXpX54CrqlMu8dddc8JMTpUM65WKZk","n":"sjL0Psd5o5TdDbiHQrzfUkrVB0YaLJvSPkJJHyM5pKx09u9hFrpSYgPhYIPUChAN_wwmE17aDPR99L9KK90CQnYuakzqtTwm2FxWOK60A1Tu4KARWz6QZsGrVoqRRTF4oDqXtIl5PhZ9iSWnswR-AEZRZowOSJYo1OkjJl9f_X6lZyuR0UYs4GrG3sHAErndUtOSiC7CU1_-Vb43CHVXSBm3iM0UKHB98nDRp5dCIjrIqGw0SfTvsKTsLwYKo7aYF26B7_sIX7HDXtrnJ7CuvLG29RLuYrOwagVuiT_yueN1VTVQ-QJZs587EnhGI0tYI6WH_vMjeEUM9Y6hyYFVlQ"}'
_RSA_384_JWK_PUB = '{"kty":"RSA","e":"AQAB","kid":"testKeyId","alg":"RS384","n":"sjL0Psd5o5TdDbiHQrzfUkrVB0YaLJvSPkJJHyM5pKx09u9hFrpSYgPhYIPUChAN_wwmE17aDPR99L9KK90CQnYuakzqtTwm2FxWOK60A1Tu4KARWz6QZsGrVoqRRTF4oDqXtIl5PhZ9iSWnswR-AEZRZowOSJYo1OkjJl9f_X6lZyuR0UYs4GrG3sHAErndUtOSiC7CU1_-Vb43CHVXSBm3iM0UKHB98nDRp5dCIjrIqGw0SfTvsKTsLwYKo7aYF26B7_sIX7HDXtrnJ7CuvLG29RLuYrOwagVuiT_yueN1VTVQ-QJZs587EnhGI0tYI6WH_vMjeEUM9Y6hyYFVlQ"}'
_ES_256_JWK = '{"kty":"EC","d":"VmWWG6AU_TTajGJvrBWnG_NaUyH9rWJjUtzzCjrRPEU","crv":"P-256","kid":"testKeyId","x":"xtu5hUhexZV77FWXdeZ4rhgE9mT9i8UPwlEpbaBfiTk","y":"medk7WxeUgrA2T0oIybFfpAoTBlzZg5wKWEz4eR-Fbc","alg":"ES256"}'
_ES_256_JWK_PUB = '{"kty":"EC","crv":"P-256","kid":"testKeyId","x":"xtu5hUhexZV77FWXdeZ4rhgE9mT9i8UPwlEpbaBfiTk","y":"medk7WxeUgrA2T0oIybFfpAoTBlzZg5wKWEz4eR-Fbc","alg":"ES256"}'

class TestMomentoSigner(unittest.TestCase):
    def test_rsa256(self):
        token = MomentoSigner(_RSA_256_JWK).sign_access_token(SigningRequest(
            cache_name="foo",
            cache_key="bar",
            cache_operation=CacheOperation.GET,
            expiry_epoch_seconds=4079276098
        ))
        verified_claims = jwt.decode(token, PyJWK.from_json(_RSA_256_JWK_PUB).key, algorithms=["RS256"])
        self.assertEqual(verified_claims, {'exp': 4079276098, 'cache': 'foo', 'key': 'bar', 'method': ['get']})

    def test_rsa384(self):
        token = MomentoSigner(_RSA_384_JWK).sign_access_token(SigningRequest(
            cache_name="foo",
            cache_key="bar",
            cache_operation=CacheOperation.GET,
            expiry_epoch_seconds=4079276098
        ))
        verified_claims = jwt.decode(token, PyJWK.from_json(_RSA_384_JWK_PUB).key, algorithms=["RS384"])
        self.assertEqual(verified_claims, {'exp': 4079276098, 'cache': 'foo', 'key': 'bar', 'method': ['get']})


    def test_es256(self):
        token = MomentoSigner(_ES_256_JWK).sign_access_token(SigningRequest(
            cache_name="foo",
            cache_key="bar",
            cache_operation=CacheOperation.GET,
            expiry_epoch_seconds=4079276098
        ))
        verified_claims = jwt.decode(token, PyJWK.from_json(_ES_256_JWK_PUB).key, algorithms=["ES256"])
        self.assertEqual(verified_claims, {'exp': 4079276098, 'cache': 'foo', 'key': 'bar', 'method': ['get']})

    def test_empty_jwk_json_string(self):
        with self.assertRaises(InvalidArgumentError):
            MomentoSigner('')

    def test_nothing_jwk_json_string(self):
        with self.assertRaises(InvalidArgumentError):
            MomentoSigner('{}')

    def test_incomplete_jwk_json_string(self):
        with self.assertRaises(InvalidArgumentError):
            MomentoSigner('{"alg":"foo","kid":"bar","kty":"RSA"}')


    def test_create_presigned_url_for_get(self):
        result = MomentoSigner(_RSA_256_JWK).create_presigned_url("example.com", SigningRequest(
            cache_name="foo",
            cache_key="bar",
            cache_operation=CacheOperation.GET,
            expiry_epoch_seconds=4079276098
        ))

        self.assertEqual(result, 'https://example.com/cache/get/foo/bar?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6InRlc3RLZXlJZCJ9.eyJleHAiOjQwNzkyNzYwOTgsImNhY2hlIjoiZm9vIiwia2V5IjoiYmFyIiwibWV0aG9kIjpbImdldCJdfQ.OmFbu3Dp0JrdXcgqlKaE9x3RR81D6_oP-ub_dNvMcifCshivsh1Mt0DdpTbeK1CRSZqAjZwQYH5s4y66J5Hu4g74VJZeVkI0E7eDiRPTM97UDIj2OYrg4KebwhPfuk5-18csjCEILZ19byUxPk3DbDKmn03olUCyzym7gxjXlDXrDF12IPY-zI1_K4n3Gct2bXdlqtMDcVWwUW7PEwjY2KpR89sx8SceS1FpeLL_lygQBkiN46f9FCjH5luMjDrDtEiDr7ttRT2rVa4jW2xjzKCQwFy6aetMMoqrphEMgl3-2LN4AsGlxf39ZlVlV7MtVfjttpYdO9zBpIgDSHiT7g')

    def test_create_presigned_url_for_set(self):
        result = MomentoSigner(_RSA_256_JWK).create_presigned_url("example.com", SigningRequest(
            cache_name="foo",
            cache_key="bar",
            cache_operation=CacheOperation.SET,
            expiry_epoch_seconds=4079276098,
            ttl_seconds=5
        ))

        self.assertEqual(result, 'https://example.com/cache/set/foo/bar?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6InRlc3RLZXlJZCJ9.eyJleHAiOjQwNzkyNzYwOTgsImNhY2hlIjoiZm9vIiwia2V5IjoiYmFyIiwibWV0aG9kIjpbInNldCJdLCJ0dGwiOjV9.IU8fKATJleYIPJyw5C4jOaXOL5TgTZbqi_5nzuGcHR3rZlZMOSfgPUvj74r0P9qmQNClKAiYwtmUTu1NnYvwParSN1ph3iaDAM19RTjXGfZtdiwdAolqUNCKSzvEC1yXsjEQaEdHhelzXAa4TN_X_488IJjjWbVFb6q4lv_JlsPaDLX2IOqG2MYAA2tiHm9-wu_KZ4cd2O-WLEicy2kqKrPu96BLvDwrXLaawG0NQFD4jP74ac1MQraPRchWbFg6qfX35AK0QouJYft65mQ_3G8h8Y-mJCZFJ0pyKydpXGpBzAyr8RmQu5t6s_1J_eX-tyOQIiLar5tS3lajc75msQ&ttl_milliseconds=5000')
