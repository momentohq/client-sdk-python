import pytest

import jwt
from jwt.api_jwk import PyJWK

from momento.momento_signer import MomentoSigner, CacheOperation, SigningRequest
from momento.errors import InvalidArgumentError
from tests.utils import uuid_str


_RSA_NO_ALG_JWK = '{"p":"8qu5SjdYE-pnqp8KxgD5VRxXVhu5bqlufsQR4ki13-nOap8hXHP5-X6dtHDef77NbjeHlSsqdoiEQzVLkTNnY2S9owshBQ_E0nq1XSf_ma4IA9d50KCQF9jHkl2npziyanJgvu33RB7NVh5cuVWzFwZqVq08oMLHiyAs_TfIqu0","kty":"RSA","q":"uiDceVagmm9JMtr4lOLTVp4VnX0evpmqUpcq7a6fl696m8cvnHT9VXNURFPagp0QfWOYGB_j30Fw1dWrnLe5IXSuK_qNgSdEm64fspN9oCuRanK7a7WYw54msr7-gsowu5mIcGaM7H-Lu7wWsA78y6nkBEsuEGWE2y-a1TLJUms","d":"CeJjOaYzZADIoU0uqKYi0jmfwbmI-AOzKo1H_GPoEdo1NvlaHcqMWnXEie8LbN1IvyC6OvknJ3saJ5em0QzjNWyv_IethCMu_kBpZnKXqBuUZ5MtNlXuSsF4J37skMe_zWehpBXun4iWozrHtugdAiYKBLB-tazkiWZ0b03CIzSLYMz7gtj2OHMCiigLEl0dhX32rfRK3-c6ax_yK7XGnT46COQkqDuqSAJrVS7i-4P5MyLIGphoSC0guWS4Vqqo_2yxE4UFEnANtGB59fRylLFunDGo-9Jvdkatj3zpSTrySUhHI9JC2us_iB_LRp7tfyMUtnYeU4g-nJEixvoYZQ","e":"AQAB","kid":"testKeyId","qi":"RBkWNYGJZ0jcEMm7KdnEkc2KDe5LxH5SAaZPe3l_tOruu4RXbLxB_BtuZ2uQSIfsUmvK4zSjhp52BCMGn_cqvNA4ZrGJ_IGRHD6P5dt4HlCg3L0xeB6PZ5risJj45KTiUqqG24HfY56QN7SpRz4vVoTLJsmrlDJq0X9G-44D1Yo","dp":"O5yJiLytqz7CtnwZJmio1wp-Pc3TsGZ4mTVK-15HJzkFFtX-WPq4Zlx_Gws67QCO8Es9yBvxc2q3qtbVuFZ7SEQ__WRHeTnVbKruEHM566N_non5B5HZs7Hx3HebLo3T7igosd49BoPWhxgwSOrPcpGF38LwiMEwSXHe-1kPt0U","dq":"VtIBRbA81gzXDhvKHFj5z8uJtZ6peqrfIgtVgO0VkIHQJV3yPX7stLFJO14J7Scqi_Kq_YXSm09BPN2gYUfp2Us9-1GyM-6HOD8ulfPqg44PFKJT_lgE3CqnTnV87rE1rixd0mBjl-We3oFL6-_xx2aF7-LJp-hS4pMAHDbGZeU","n":"sG_rFa4BMxYupx7ru5WPjoRE-81Jomd8g4jx7WAmVSe2uvnEC57zfBYaZ8saULkGwd9A7dzvw1skRW8RTmL1qttjM2ZTlsMaa_bpS1a2PloNZfIKNzux0KOkKwbNdnO3bPpAMrsgAIC6BFom9ISdCrTP1cgbe-klAp04osEsK0jNNglJhZFIIBzwqbYvGGLaat3ribY-OB9KN3Vhh9Z7v7F-i9dobbZk68nVUd0sgGZ-ht3xF-mdnN-CtugZjO0_Ke7t0jIRu_qNvZsbi9MqhSB6FRhqkFcs4n5HNxu082OwraU7MMZWbDjRYeq01MlGKGFPd-8xJoZ93bDWFbRbDw"}'
_RSA_NO_ALG_JWK_PUB = '{"kty":"RSA","e":"AQAB","kid":"testKeyId","n":"sG_rFa4BMxYupx7ru5WPjoRE-81Jomd8g4jx7WAmVSe2uvnEC57zfBYaZ8saULkGwd9A7dzvw1skRW8RTmL1qttjM2ZTlsMaa_bpS1a2PloNZfIKNzux0KOkKwbNdnO3bPpAMrsgAIC6BFom9ISdCrTP1cgbe-klAp04osEsK0jNNglJhZFIIBzwqbYvGGLaat3ribY-OB9KN3Vhh9Z7v7F-i9dobbZk68nVUd0sgGZ-ht3xF-mdnN-CtugZjO0_Ke7t0jIRu_qNvZsbi9MqhSB6FRhqkFcs4n5HNxu082OwraU7MMZWbDjRYeq01MlGKGFPd-8xJoZ93bDWFbRbDw"}'
_RSA_256_JWK = '{"p":"4Xhu9y3DCDO6ttlDrb5JuSk9-F6oKIE7y3zDxObR1UbLBPua4X3qW82VGYjzn6yO7_qRECzN-K1LLd6yIwK5L7i6rIglP_z-Kyzp-UwacQrnvvf0ZM1xE2E6JUAkevXf1khCXAuKd04S9NxEKv0gDvwdspw-WwqiOMVU1gn1aF0","kty":"RSA","q":"0Pyj5MqoVGuUSGYL0-HLYBspD7vgi9G8HjNh8MQC1AF5w58k29dtAH2-dj3IO3b_CrNisKyMpYLT1OJBsYaJGi2TZRnBGbZb-LyZTz5vDBu8f3SgUxSHcqUsVOO7Pr555OJKdNRr3ag3m8RwIn3Agbvrs0HQPM-XkfUI7p7Bc28","d":"EqUgXXSG8gWx2wGJpjKZTRMd7lzuct96AyzWBsCTmYAVj6BsczKERFUFtoUs6kgF4aZiq8ym-mNQdVvane6aeK3p0F5RfvGwtGg7xkavNidtOf51fEPa8yCXLIwFclM_tav4Y_LlGdiXNsXVX6X8ZgTiS5vUCRzt_5e8Nks1T1kMPFdiW_9F-emXWrl5hmrIiG1XpBX4sG6YNGWty1u7Reu9a8ydzMmcFSuxE5bJodDPE1ZiZOQOtVxk3TVAKUEQ7SqOksN_w_FX_6Du5bC8cjz38dJhfL3R9B_FQA5oM_nO1XklgigF8lg8_b2sTb-OQc6dY6qE241ZknwxDbKIIQ","e":"AQAB","kid":"testKeyId","qi":"0NHMRRg_aQT8hKItg1HxnUqgWHHb4S92DqhBY5AERmOPUUQvK7qkpc0DN4OVQwZGpl0_lBqsadjoCROrJ6HrVMkx0fkoNPQIKRzT_ew6ORXPPrB5d2elS4c93mjYZjvQtxdpQKtyM4_9Va1v6PRa2IGUSPzJt6h8BQBM9NQhHuY","dp":"aWizgA2942TDwt46HM0cjFsypJ4kQaOBf_WZVMGQkgQhv_edBhSm7zpinWiAdULoJFthXE2GEd96iTxWzbVlPGFBrI2N1KeDcE30KN-icPznMUmc0U-WsLfAxk-BfpbaicSIeZ3Po0014ZHksLBcP4UwoSMYp9mF08K1kcdgGuU","alg":"RS256","dq":"DsvUTr6KbG-xb-7Jp5a073j8z0BeBYgz6W9537IBAUGZfWAnG-mEriQ49-Yn5w3lwLwyoI-W5aD9nnTmccs0qcXQSbgpE8j1egbgU9v3wMO19NAtCbTKYjOPj_MPrsGNn8blvp_Lg0YFqeGejtKYbpb_eRGPzL5l3M-cckiLKcE","n":"uBBdD0DnAeArN2cqaG9zUDmZU_hEen7glZN0ssVSf_ZML_HdEu7BR5G9gGToYQ7dkH010FzciZMbouXUDovd71snXqlvnwyvqpU3UUfVgudmN-CDa97170WnjpjIQLVErQCX-3_PIctJweDmJLzaQsmQCPoSzZkiQ17NsMwN-xw2L-eRfwmyOWKv0mRkDpZRYgn4rrm70hepOZp0-YEpEC_vTYykc2WVuvEVez_9nF-VwXGVX-a1_zChIAz5V_-VXIqtoL7ot945wAEyEhtV4Yqr5LoSL8mvSL3v7WlF56NqaltDWe2yrowlZiQ2EDwwUKxArcYd7BnNmdYqo2cHUw"}'
_RSA_256_JWK_PUB = '{"kty":"RSA","e":"AQAB","kid":"testKeyId","alg":"RS256","n":"uBBdD0DnAeArN2cqaG9zUDmZU_hEen7glZN0ssVSf_ZML_HdEu7BR5G9gGToYQ7dkH010FzciZMbouXUDovd71snXqlvnwyvqpU3UUfVgudmN-CDa97170WnjpjIQLVErQCX-3_PIctJweDmJLzaQsmQCPoSzZkiQ17NsMwN-xw2L-eRfwmyOWKv0mRkDpZRYgn4rrm70hepOZp0-YEpEC_vTYykc2WVuvEVez_9nF-VwXGVX-a1_zChIAz5V_-VXIqtoL7ot945wAEyEhtV4Yqr5LoSL8mvSL3v7WlF56NqaltDWe2yrowlZiQ2EDwwUKxArcYd7BnNmdYqo2cHUw"}'
_RSA_384_JWK = '{"p":"2UqxTsQeIH2wsAP7FsDltFNGkco526v1nYjTSguReJr11klvMjju5H2cm6vrJGURQ6-e_yvyAzM7avG-D2o7ZGrYQ-pJXwF-lS7Nzwm7XWLvNv78vyH95aIdWh1KaeaBswN-JbGyamcmGgtwO_0ICTOMmyGpku38b52CHVgq-Dc","kty":"RSA","q":"0fF9Fnv2CHso23K13AcZCf5eTFy2tk9zd3zhheq_IbnvEjPiMp_UXAozd3Lh8MZMXpM0HhB1Zpxs89eF8Pyy3GwWq08F2lMOxDGaYxeTU258Kq2KFPQ8VDsY6SGuAUAGkwYM_IXX0_ixt3FqrJIAuLI9pPAkymV9Hi_oq-FOopM","d":"SNx9spekPEb3VY9gOuD4HPE9BkLbG-dAgTVgKYuhG5xqHLeHBqB7Y9k7U1fDoLOszV59yQjM9mQXhjWa0bGFpYAl_U_i3eSw62Fh3b5NG1mkbJL2Vc0QhPtmH7X7rQOL01PxyJ83RDQ08xlC5vcMv_cAfIICqxo-qsOCnEN9_6sUrPQkv19oakwfMq9ACXDWPgkHNwWTBbyO7CtUCf6chYLPr13MsjLe_FSqY0rrNKr68qq0WtXFaldJR1SeE_lEd7pR1kAMoX0rm57yNqBljsgMweOLMz6swulIRZBWtmEfxvvYL0H5RnmwSbz00NSjErqwv518KPpNzS9O8qE-dw","e":"AQAB","kid":"testKeyId","qi":"nxcUfqj-jKFtGmGKIQo8b4M3eOnXdpm0QWyfgazzk_v1uhpbGg_VC7ZKxqY1DMfQHyjxFEz8a3mFsusVvwsCFuSzmqp36-f7tuXiAVfJjHs8cNIVZbO0GU1iRzd21yfBuj1JIr2FoU2g_1G--h48-tWXOmrPmBc9_xTSfjEeNGE","dp":"DiCqGIntv4UMiNUpbRhLlwbXDsGMM3khtgVgX28THTlOBImvvh8vgRGdrg1mc25SygjQGJ0d1hFtqo1fIxdwFx5PQ1MnRBMPzNlHLk_eq7qz_OplOnQWUujQabx_yxTel-oBOKguBncAZi8aM_xGmnqMiMWOhewNPqCKBihmWs8","alg":"RS384","dq":"zV2YqyHfbjRrpx7y3qTizW_R9ojLAlN98-hpA4K6LNehEQFHx5WpOc-QwMvUUJ7pnaoJVU9sSE_EFFNDZpUKsavaEQFgDE0rKKgNCdnJ99cgBu9zH0Q6r3qPx512hSqIQ9GramnS0jt4PKXpX54CrqlMu8dddc8JMTpUM65WKZk","n":"sjL0Psd5o5TdDbiHQrzfUkrVB0YaLJvSPkJJHyM5pKx09u9hFrpSYgPhYIPUChAN_wwmE17aDPR99L9KK90CQnYuakzqtTwm2FxWOK60A1Tu4KARWz6QZsGrVoqRRTF4oDqXtIl5PhZ9iSWnswR-AEZRZowOSJYo1OkjJl9f_X6lZyuR0UYs4GrG3sHAErndUtOSiC7CU1_-Vb43CHVXSBm3iM0UKHB98nDRp5dCIjrIqGw0SfTvsKTsLwYKo7aYF26B7_sIX7HDXtrnJ7CuvLG29RLuYrOwagVuiT_yueN1VTVQ-QJZs587EnhGI0tYI6WH_vMjeEUM9Y6hyYFVlQ"}'
_RSA_384_JWK_PUB = '{"kty":"RSA","e":"AQAB","kid":"testKeyId","alg":"RS384","n":"sjL0Psd5o5TdDbiHQrzfUkrVB0YaLJvSPkJJHyM5pKx09u9hFrpSYgPhYIPUChAN_wwmE17aDPR99L9KK90CQnYuakzqtTwm2FxWOK60A1Tu4KARWz6QZsGrVoqRRTF4oDqXtIl5PhZ9iSWnswR-AEZRZowOSJYo1OkjJl9f_X6lZyuR0UYs4GrG3sHAErndUtOSiC7CU1_-Vb43CHVXSBm3iM0UKHB98nDRp5dCIjrIqGw0SfTvsKTsLwYKo7aYF26B7_sIX7HDXtrnJ7CuvLG29RLuYrOwagVuiT_yueN1VTVQ-QJZs587EnhGI0tYI6WH_vMjeEUM9Y6hyYFVlQ"}'
_ES_256_JWK = '{"kty":"EC","d":"VmWWG6AU_TTajGJvrBWnG_NaUyH9rWJjUtzzCjrRPEU","crv":"P-256","kid":"testKeyId","x":"xtu5hUhexZV77FWXdeZ4rhgE9mT9i8UPwlEpbaBfiTk","y":"medk7WxeUgrA2T0oIybFfpAoTBlzZg5wKWEz4eR-Fbc","alg":"ES256"}'
_ES_256_JWK_PUB = '{"kty":"EC","crv":"P-256","kid":"testKeyId","x":"xtu5hUhexZV77FWXdeZ4rhgE9mT9i8UPwlEpbaBfiTk","y":"medk7WxeUgrA2T0oIybFfpAoTBlzZg5wKWEz4eR-Fbc","alg":"ES256"}'
_ES_NO_ALG_JWK = '{"kty":"EC","d":"ZhrhvO1Zk8ENkqlDXpHrEJ2TWgZhPSyjgX0j-8jUWig","crv":"P-256","kid":"testKeyId","x":"5BU5xuaUvasp9gUfSS3HGtqd1oHdGoHH3KtrzoQLd0Q","y":"WUjUeDikRXRHa-AWyNdH5Ye1Nyifd3P26F52Uv4eTVo"}'
_ES_NO_ALG_JWK_PUB = '{"kty":"EC","crv":"P-256","kid":"testKeyId","x":"5BU5xuaUvasp9gUfSS3HGtqd1oHdGoHH3KtrzoQLd0Q","y":"WUjUeDikRXRHa-AWyNdH5Ye1Nyifd3P26F52Uv4eTVo"}'


def test_rsa256():
    cache_name = uuid_str()
    cache_key = uuid_str()

    token = MomentoSigner(_RSA_256_JWK).sign_access_token(
        SigningRequest(
            cache_name=cache_name,
            cache_key=cache_key,
            cache_operation=CacheOperation.GET,
            expiry_epoch_seconds=4079276098,
        )
    )
    verified_claims = jwt.decode(
        token, PyJWK.from_json(_RSA_256_JWK_PUB).key, algorithms=["RS256"]
    )

    assert verified_claims == {
        "exp": 4079276098,
        "cache": cache_name,
        "key": cache_key,
        "method": ["get"],
    }


def test_rsa_no_alg():
    cache_name = uuid_str()
    cache_key = uuid_str()

    token = MomentoSigner(_RSA_NO_ALG_JWK).sign_access_token(
        SigningRequest(
            cache_name=cache_name,
            cache_key=cache_key,
            cache_operation=CacheOperation.GET,
            expiry_epoch_seconds=4079276098,
        )
    )
    verified_claims = jwt.decode(
        token, PyJWK.from_json(_RSA_NO_ALG_JWK_PUB).key, algorithms=["RS256"]
    )

    assert verified_claims == {
        "exp": 4079276098,
        "cache": cache_name,
        "key": cache_key,
        "method": ["get"],
    }


def test_rsa384():
    cache_name = uuid_str()
    cache_key = uuid_str()

    token = MomentoSigner(_RSA_384_JWK).sign_access_token(
        SigningRequest(
            cache_name=cache_name,
            cache_key=cache_key,
            cache_operation=CacheOperation.GET,
            expiry_epoch_seconds=4079276098,
        )
    )
    verified_claims = jwt.decode(
        token, PyJWK.from_json(_RSA_384_JWK_PUB).key, algorithms=["RS384"]
    )

    assert verified_claims == {
        "exp": 4079276098,
        "cache": cache_name,
        "key": cache_key,
        "method": ["get"],
    }


def test_es256():
    cache_name = uuid_str()
    cache_key = uuid_str()

    token = MomentoSigner(_ES_256_JWK).sign_access_token(
        SigningRequest(
            cache_name=cache_name,
            cache_key=cache_key,
            cache_operation=CacheOperation.GET,
            expiry_epoch_seconds=4079276098,
        )
    )
    verified_claims = jwt.decode(
        token, PyJWK.from_json(_ES_256_JWK_PUB).key, algorithms=["ES256"]
    )

    assert verified_claims == {
        "exp": 4079276098,
        "cache": cache_name,
        "key": cache_key,
        "method": ["get"],
    }


def test_es_no_alg():
    cache_name = uuid_str()
    cache_key = uuid_str()

    token = MomentoSigner(_ES_NO_ALG_JWK).sign_access_token(
        SigningRequest(
            cache_name=cache_name,
            cache_key=cache_key,
            cache_operation=CacheOperation.GET,
            expiry_epoch_seconds=4079276098,
        )
    )
    verified_claims = jwt.decode(
        token, PyJWK.from_json(_ES_NO_ALG_JWK_PUB).key, algorithms=["ES256"]
    )

    assert verified_claims == {
        "exp": 4079276098,
        "cache": cache_name,
        "key": cache_key,
        "method": ["get"],
    }


def test_empty_jwk_json_string():
    with pytest.raises(InvalidArgumentError):
        MomentoSigner("")


def test_nothing_jwk_json_string():
    with pytest.raises(InvalidArgumentError):
        MomentoSigner("{}")


def test_incomplete_jwk_json_string():
    with pytest.raises(InvalidArgumentError):
        MomentoSigner('{"alg":"foo","kid":"bar","kty":"RSA"}')


def test_create_presigned_url_for_get():
    result = MomentoSigner(_RSA_256_JWK).create_presigned_url(
        "example.com",
        SigningRequest(
            cache_name="!#$&'()*+,/:;=?@[]",
            cache_key="!#$&'()*+,/:;=?@[]",
            cache_operation=CacheOperation.GET,
            expiry_epoch_seconds=4079276098,
        ),
    )

    assert (
        result
        == "https://rest.example.com/cache/get/%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%40%5B%5D/%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%40%5B%5D?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6InRlc3RLZXlJZCJ9.eyJleHAiOjQwNzkyNzYwOTgsImNhY2hlIjoiISMkJicoKSorLC86Oz0_QFtdIiwia2V5IjoiISMkJicoKSorLC86Oz0_QFtdIiwibWV0aG9kIjpbImdldCJdfQ.TpM6MLCnEWKJfwy0Mp5n9c9ygS5KwklpHqNTTCCncICTgENblbz3BGMUXUw4ljrTCt0uwxebX2iIROMGP32SevMlcygxsnXweHHVvkONAeok5ASvhiQtJcClb_4BMCkxPL7OlmaDlJVrcWdIqNa5E_HG6IWA6TXDJDzRrNPvknD7TVMRYDxaUMagdF3kPMBXZeO6CdlhLGb6Kfhmyc2mkdt8o-aCK3-n2vIXqiEwRNkSVr2iGBlP1l6nlVQ_dXfb0I56fLTncF1xWI1zDf2pbiQn9S3Z4n45_0C2yZy_FI8csWM2gYNqKK5VqsxkpbhlFJWyINxnyNA-FMBDhdJUZQ"
    )


def test_create_presigned_url_for_set():
    result = MomentoSigner(_RSA_256_JWK).create_presigned_url(
        "example.com",
        SigningRequest(
            cache_name="!#$&'()*+,/:;=?@[]",
            cache_key="!#$&'()*+,/:;=?@[]",
            cache_operation=CacheOperation.SET,
            expiry_epoch_seconds=4079276098,
            ttl_seconds=5,
        ),
    )

    assert (
        result
        == "https://rest.example.com/cache/set/%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%40%5B%5D/%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%40%5B%5D?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6InRlc3RLZXlJZCJ9.eyJleHAiOjQwNzkyNzYwOTgsImNhY2hlIjoiISMkJicoKSorLC86Oz0_QFtdIiwia2V5IjoiISMkJicoKSorLC86Oz0_QFtdIiwibWV0aG9kIjpbInNldCJdLCJ0dGwiOjV9.GlsGrxuoMMvwyr-SHBxfAK_CEk55P218jcsBTW9PiXoYgd85BNuDaHcQJaE_31CRdJ5emXj_qIQZjFLz3LDb3zHSAHCSYzg_pDZyVB-yLaW4nOCiztaxlr_FsihgghHUziO2lFyPgNpx2iZUQ5RnUvaCkhwN8R-FbKhBQ4Oh8hG4xBuILEIA5fJ8PAhbvmqzgmgbzplbhPMVvNPVXbdEn5YCdqIuoo6oQTB8ksgm788d7zRBgJmcyF07lDviGFaXt7OYshBWxKZ8f8Iv9PTaDtIFWPJDdaYCTcaYoaOqA2VXFEFmqcuDwcRIaNGkaYd8emqnlKc4ItdASLWV5k1Wjg&ttl_milliseconds=5000"
    )


def test_create_presigned_url_for_set_missing_ttl():
    with pytest.raises(InvalidArgumentError):
        MomentoSigner(_RSA_256_JWK).create_presigned_url(
            "example.com",
            SigningRequest(
                cache_name="!#$&'()*+,/:;=?@[]",
                cache_key="!#$&'()*+,/:;=?@[]",
                cache_operation=CacheOperation.SET,
                expiry_epoch_seconds=4079276098,
            ),
        )
