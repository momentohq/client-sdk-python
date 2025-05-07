# Changelog

## [1.27.0](https://github.com/momentohq/client-sdk-python/compare/v1.26.0...v1.27.0) (2025-05-07)


### Features

* Add a credential provider for Momento Local ([#501](https://github.com/momentohq/client-sdk-python/issues/501)) ([9f809c8](https://github.com/momentohq/client-sdk-python/commit/9f809c84bfbf35ce8576da773f419b7df741da20))
* add middleware ([#502](https://github.com/momentohq/client-sdk-python/issues/502)) ([1230510](https://github.com/momentohq/client-sdk-python/commit/1230510154681507a8cb6a58a28ffde8038e12e8))


### Miscellaneous

* add tests for the fixed count retry strategy ([#503](https://github.com/momentohq/client-sdk-python/issues/503)) ([ac7bdb8](https://github.com/momentohq/client-sdk-python/commit/ac7bdb8f59adc08000a9a509b4873777ad97fdc0))
* **deps:** bump momento from 1.25.0 to 1.26.0 in /examples ([#499](https://github.com/momentohq/client-sdk-python/issues/499)) ([a0a1382](https://github.com/momentohq/client-sdk-python/commit/a0a1382a66aa751354d19fd0f59d15388501729e))
* fixed timeout retry strategy ([#504](https://github.com/momentohq/client-sdk-python/issues/504)) ([5f1398f](https://github.com/momentohq/client-sdk-python/commit/5f1398fc45551c4afbb4180395e0166872a63f8c))
* remove configuration bases and make middleware more pythonic ([#505](https://github.com/momentohq/client-sdk-python/issues/505)) ([6b810cc](https://github.com/momentohq/client-sdk-python/commit/6b810cca96a3d170adb13de494b3bed267f995f9))
* send only-once headers per client instead of just once ([#507](https://github.com/momentohq/client-sdk-python/issues/507)) ([11da1a5](https://github.com/momentohq/client-sdk-python/commit/11da1a5c20f483b6772c675a51a02f6d7b786df9))

## [1.26.0](https://github.com/momentohq/client-sdk-python/compare/v1.25.0...v1.26.0) (2025-03-06)


### Features

* add topic grpc config and transport strategy and make sure publish deadline is set ([#496](https://github.com/momentohq/client-sdk-python/issues/496)) ([bfac90d](https://github.com/momentohq/client-sdk-python/commit/bfac90d0c235ef79272ddbb54ad80b3ae7d65c82))


### Bug Fixes

* disable dynamic DNS service config ([#497](https://github.com/momentohq/client-sdk-python/issues/497)) ([d677563](https://github.com/momentohq/client-sdk-python/commit/d677563afe032e83324c5aff8e64d20152f622a1))


### Miscellaneous

* **deps:** bump momento from 1.23.3 to 1.25.0 in /examples ([#493](https://github.com/momentohq/client-sdk-python/issues/493)) ([3e1d086](https://github.com/momentohq/client-sdk-python/commit/3e1d08636d087e309089b6d91810505ff9bb6b33))
* update list of retryable gRPC functions ([#495](https://github.com/momentohq/client-sdk-python/issues/495)) ([b8e527c](https://github.com/momentohq/client-sdk-python/commit/b8e527c8c3b6335ecf1966cc7cd63702088e3f99))

## [1.25.0](https://github.com/momentohq/client-sdk-python/compare/v1.24.0...v1.25.0) (2024-11-21)


### Features

* support topic sequence page ([#492](https://github.com/momentohq/client-sdk-python/issues/492)) ([d8e5039](https://github.com/momentohq/client-sdk-python/commit/d8e5039007f72794a23680cd53602b23076d71ad))


### Miscellaneous

* **deps-dev:** bump braces in /examples/lambda/infrastructure ([#484](https://github.com/momentohq/client-sdk-python/issues/484)) ([03601a1](https://github.com/momentohq/client-sdk-python/commit/03601a1e5faa877a98b1e9d28eb7fd9e6e9d1062))
* improve resource exhausted error message ([#485](https://github.com/momentohq/client-sdk-python/issues/485)) ([b4439bd](https://github.com/momentohq/client-sdk-python/commit/b4439bd1b707a450b9c02a1b821579111105b115))
* release-please workflow should pick up feat, fix, and chore commits ([#486](https://github.com/momentohq/client-sdk-python/issues/486)) ([6e975cb](https://github.com/momentohq/client-sdk-python/commit/6e975cb4dea071147573cb18dce2d2af4b3f8878))
* specify path to release-please manifest ([#487](https://github.com/momentohq/client-sdk-python/issues/487)) ([227aa40](https://github.com/momentohq/client-sdk-python/commit/227aa40697d3604ef7d720e52aabdaec348855dc))
* update license file ([#488](https://github.com/momentohq/client-sdk-python/issues/488)) ([bb61d81](https://github.com/momentohq/client-sdk-python/commit/bb61d81653921952022337fb608d645f6a67924d))
* upgrade proto dependency version ([#489](https://github.com/momentohq/client-sdk-python/issues/489)) ([77b855f](https://github.com/momentohq/client-sdk-python/commit/77b855f9dec8311c5830e6189200a9a9de7b08d7))

## [1.24.0](https://github.com/momentohq/client-sdk-python/compare/v1.23.5...v1.24.0) (2024-09-27)


### Features

* read version from package init ([#481](https://github.com/momentohq/client-sdk-python/issues/481)) ([6ca2525](https://github.com/momentohq/client-sdk-python/commit/6ca2525051892159db3673892fcac3cad08a567b))


### Bug Fixes

* lint errors on 3.10+ for auth protos ([#479](https://github.com/momentohq/client-sdk-python/issues/479)) ([e3be891](https://github.com/momentohq/client-sdk-python/commit/e3be89171ff4284b68801a86320aedfd912b3a42))

## [1.23.5](https://github.com/momentohq/client-sdk-python/compare/v1.23.4...v1.23.5) (2024-09-27)


### Miscellaneous Chores

* set release-please base version as 1.23.4 ([#477](https://github.com/momentohq/client-sdk-python/issues/477)) ([06869b2](https://github.com/momentohq/client-sdk-python/commit/06869b2316406b875bcdf2535b13c373ae8cecfe))
